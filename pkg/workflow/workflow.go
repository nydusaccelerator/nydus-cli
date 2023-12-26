package workflow

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nydusaccelerator/nydus-cli/pkg/backend"
	"github.com/nydusaccelerator/nydus-cli/pkg/container"
	"github.com/nydusaccelerator/nydus-cli/pkg/diff"
	"github.com/nydusaccelerator/nydus-cli/pkg/distribution"
	parserPkg "github.com/nydusaccelerator/nydus-cli/pkg/nydus/parser"
	"github.com/nydusaccelerator/nydus-cli/pkg/nydus/utils"
	"github.com/nydusaccelerator/nydus-cli/pkg/remote"
	"golang.org/x/sync/errgroup"

	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/nydus-snapshotter/pkg/converter"
	"github.com/dustin/go-humanize"
	"github.com/nydusaccelerator/nydus-cli/pkg/config"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const layerAnnotationNydusCommitBlobs = "containerd.io/snapshot/nydus-commit-blobs"
const layerAnnotationNydusBlobIDs = "containerd.io/snapshot/nydus-blob-ids"

type Workflow struct {
	cfg     *config.Config
	workDir string
	cm      *container.Manager
	be      backend.Backend
	beMutex sync.Mutex
}

type Blob struct {
	Name          string
	BootstrapName string
	Desc          ocispec.Descriptor
}

type CommitOption struct {
	ContainerIDWithType string
	TargetRef           string
	WithPaths           []string
	WithoutPaths        []string
	PauseContainer      bool
	MaximumTimes        int
}

func calcDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", errors.Wrap(err, "open file")
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", errors.Wrap(err, "calc file sha256")
	}

	digest := fmt.Sprintf("%x", h.Sum(nil))

	return digest, nil
}

func NewWorkflow(cfg *config.Config) (*Workflow, error) {
	if err := os.MkdirAll(cfg.Base.WorkDir, 0755); err != nil {
		return nil, errors.Wrap(err, "prepare work dir")
	}

	workDir, err := os.MkdirTemp(cfg.Base.WorkDir, "nydus-cli-")
	if err != nil {
		return nil, errors.Wrap(err, "create temp dir")
	}

	cm, err := container.NewManager(&cfg.Base.Runtime)
	if err != nil {
		return nil, errors.Wrap(err, "new container manager")
	}

	return &Workflow{
		cfg:     cfg,
		workDir: workDir,
		cm:      cm,
	}, nil
}

func (wf *Workflow) backend(ref string) (backend.Backend, error) {
	wf.beMutex.Lock()
	defer wf.beMutex.Unlock()

	if wf.be != nil {
		return wf.be, nil
	}

	var err error
	if wf.cfg.OSS.Endpoint != "" {
		wf.be, err = backend.NewOSSBackend(&wf.cfg.OSS, false)
		if err != nil {
			return nil, errors.Wrap(err, "new oss backend")
		}
	} else {
		remoter, err := remote.New(ref, wf.resolverFunc)
		if err != nil {
			return nil, errors.Wrap(err, "create remote")
		}
		wf.be, err = backend.NewRegistryBackend(remoter)
		if err != nil {
			return nil, errors.Wrap(err, "new registry backend")
		}
	}
	return wf.be, nil
}

func (wf *Workflow) resolverFunc(plainHTTP bool) remotes.Resolver {
	return remote.NewResolver(true, plainHTTP, func(ref string) (string, string, error) {
		return wf.cfg.Distribution.Username, wf.cfg.Distribution.Password, nil
	})
}

func (wf *Workflow) pullBootstrap(ctx context.Context, ref, bootstrapName string) (*parserPkg.Image, int, error) {
	remoter, err := remote.New(ref, wf.resolverFunc)
	if err != nil {
		return nil, 0, errors.Wrap(err, "create remote")
	}

	parser, err := parserPkg.New(remoter, "amd64")
	if err != nil {
		return nil, 0, errors.Wrap(err, "create parser")
	}

	parsed, err := parser.Parse(ctx)
	if err != nil {
		return nil, 0, errors.Wrap(err, "parse nydus image")
	}
	if parsed.NydusImage == nil {
		return nil, 0, fmt.Errorf("not a nydus image: %s", ref)
	}

	bootstrapDesc := parserPkg.FindNydusBootstrapDesc(&parsed.NydusImage.Manifest)
	if bootstrapDesc == nil {
		return nil, 0, fmt.Errorf("not found nydus bootstrap layer")
	}
	committedLayers := 0
	_commitBlobs := bootstrapDesc.Annotations[layerAnnotationNydusCommitBlobs]
	if _commitBlobs != "" {
		committedLayers = len(strings.Split(_commitBlobs, ","))
		logrus.Infof("detected the committed layers: %d", committedLayers)
	}

	target := filepath.Join(wf.workDir, bootstrapName)
	reader, err := parser.PullNydusBootstrap(ctx, parsed.NydusImage)
	if err != nil {
		return nil, 0, errors.Wrap(err, "pull bootstrap layer")
	}
	defer reader.Close()

	if err := utils.UnpackFile(reader, utils.BootstrapFileNameInLayer, target); err != nil {
		return nil, 0, errors.Wrap(err, "unpack bootstrap layer")
	}

	return parsed.NydusImage, committedLayers, nil
}

func (wf *Workflow) commitUpperByDiff(ctx context.Context, appendMount func(path string), withPaths []string, withoutPaths []string, lowerDirs, upperDir, blobName string) (*digest.Digest, error) {
	logrus.Infof("committing upper")
	start := time.Now()

	blobPath := filepath.Join(wf.workDir, blobName)
	blob, err := os.Create(blobPath)
	if err != nil {
		return nil, errors.Wrap(err, "create upper blob file")
	}
	defer blob.Close()

	digester := digest.SHA256.Digester()
	counter := Counter{}
	tarWc, err := converter.Pack(ctx, io.MultiWriter(blob, digester.Hash(), &counter), converter.PackOption{
		WorkDir:     wf.workDir,
		FsVersion:   "5",
		Compressor:  "lz4_block",
		BuilderPath: wf.cfg.Base.Builder,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize pack to blob")
	}

	if err := diff.Diff(ctx, appendMount, withPaths, withoutPaths, tarWc, lowerDirs, upperDir); err != nil {
		return nil, errors.Wrap(err, "make diff")
	}

	if err := tarWc.Close(); err != nil {
		return nil, errors.Wrap(err, "pack to blob")
	}

	blobDigest := digester.Digest()
	logrus.Infof("committed upper, size: %s, elapsed: %s", humanize.Bytes(uint64(counter.Size())), time.Since(start))

	return &blobDigest, nil
}

func (wf *Workflow) mergeBootstrap(
	ctx context.Context, upperBlob Blob, mountBlobs []Blob, baseBootstrapName, mergedBootstrapName string,
) ([]digest.Digest, *digest.Digest, error) {
	baseBootstrap := filepath.Join(wf.workDir, baseBootstrapName)
	upperBlobRa, err := local.OpenReader(filepath.Join(wf.workDir, upperBlob.Name))
	if err != nil {
		return nil, nil, errors.Wrap(err, "open reader for upper blob")
	}

	mergedBootstrap := filepath.Join(wf.workDir, mergedBootstrapName)
	bootstrap, err := os.Create(mergedBootstrap)
	if err != nil {
		return nil, nil, errors.Wrap(err, "create upper blob file")
	}
	defer bootstrap.Close()

	digester := digest.SHA256.Digester()
	writer := io.MultiWriter(bootstrap, digester.Hash())

	layers := []converter.Layer{}
	layers = append(layers, converter.Layer{
		Digest:   upperBlob.Desc.Digest,
		ReaderAt: upperBlobRa,
	})
	for idx := range mountBlobs {
		mountBlob := mountBlobs[idx]
		mountBlobRa, err := local.OpenReader(filepath.Join(wf.workDir, mountBlob.Name))
		if err != nil {
			return nil, nil, errors.Wrap(err, "open reader for mount blob")
		}
		layers = append(layers, converter.Layer{
			Digest:   mountBlob.Desc.Digest,
			ReaderAt: mountBlobRa,
		})
	}

	blobDigests, err := converter.Merge(ctx, layers, writer, converter.MergeOption{
		WorkDir:             wf.workDir,
		FsVersion:           "5",
		ParentBootstrapPath: baseBootstrap,
		WithTar:             true,
		BuilderPath:         wf.cfg.Base.Builder,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "merge bootstraps")
	}
	bootstrapDiffID := digester.Digest()

	return blobDigests, &bootstrapDiffID, nil
}

func (wf *Workflow) pushBlob(ctx context.Context, blobName string, blobDigest digest.Digest, targetRef string) (*ocispec.Descriptor, error) {
	blobRa, err := local.OpenReader(filepath.Join(wf.workDir, blobName))
	if err != nil {
		return nil, errors.Wrap(err, "open reader for upper blob")
	}

	blobDesc := ocispec.Descriptor{
		Digest:    blobDigest,
		Size:      blobRa.Size(),
		MediaType: utils.MediaTypeNydusBlob,
		Annotations: map[string]string{
			utils.LayerAnnotationUncompressed: blobDigest.String(),
			utils.LayerAnnotationNydusBlob:    "true",
		},
	}

	backend, err := wf.backend(targetRef)
	if err != nil {
		return nil, err
	}

	return &blobDesc, backend.Push(ctx, blobRa, blobDesc)
}

func (wf *Workflow) makeDesc(ctx context.Context, x interface{}, oldDesc ocispec.Descriptor) ([]byte, *ocispec.Descriptor, error) {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		return nil, nil, errors.Wrap(err, "json marshal")
	}
	dgst := digest.SHA256.FromBytes(data)

	newDesc := oldDesc
	newDesc.Size = int64(len(data))
	newDesc.Digest = dgst

	return data, &newDesc, nil
}

func (wf *Workflow) pushManifest(
	ctx context.Context, nydusImage parserPkg.Image, bootstrapDiffID digest.Digest, targetRef, bootstrapName string, blobDigests []digest.Digest, upperBlob *Blob, mountBlobs []Blob,
) error {
	lowerBlobLayers := []ocispec.Descriptor{}
	for idx := range nydusImage.Manifest.Layers {
		layer := nydusImage.Manifest.Layers[idx]
		if layer.MediaType == utils.MediaTypeNydusBlob {
			lowerBlobLayers = append(lowerBlobLayers, layer)
		}
	}

	// Push image config
	config := nydusImage.Config
	if wf.be.External() {
		config.RootFS.DiffIDs = []digest.Digest{bootstrapDiffID}
	} else {
		config.RootFS.DiffIDs = []digest.Digest{}
		for idx := range lowerBlobLayers {
			config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, lowerBlobLayers[idx].Digest)
		}
		for idx := range mountBlobs {
			mountBlob := mountBlobs[idx]
			config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, mountBlob.Desc.Digest)
		}
		config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, upperBlob.Desc.Digest)
		config.RootFS.DiffIDs = append(config.RootFS.DiffIDs, bootstrapDiffID)
	}

	configBytes, configDesc, err := wf.makeDesc(ctx, config, nydusImage.Manifest.Config)
	if err != nil {
		return errors.Wrap(err, "make config desc")
	}

	remoter, err := remote.New(targetRef, wf.resolverFunc)
	if err != nil {
		return errors.Wrap(err, "create remote")
	}

	if err := remoter.Push(ctx, *configDesc, true, bytes.NewReader(configBytes)); err != nil {
		if remote.RetryWithHTTP(err) {
			remoter.MaybeWithHTTP(err)
			if err := remoter.Push(ctx, *configDesc, true, bytes.NewReader(configBytes)); err != nil {
				return errors.Wrap(err, "push image config")
			}
		} else {
			return errors.Wrap(err, "push image config")
		}
	}

	// Push bootstrap layer
	bootstrapTarPath := filepath.Join(wf.workDir, bootstrapName)
	bootstrapTar, err := os.Open(bootstrapTarPath)
	if err != nil {
		return errors.Wrap(err, "open bootstrap tar file")
	}

	bootstrapTarGzPath := filepath.Join(wf.workDir, bootstrapName+".gz")
	bootstrapTarGz, err := os.Create(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrap(err, "create bootstrap tar.gz file")
	}
	defer bootstrapTarGz.Close()

	digester := digest.SHA256.Digester()
	gzWriter := gzip.NewWriter(io.MultiWriter(bootstrapTarGz, digester.Hash()))
	if _, err := io.Copy(gzWriter, bootstrapTar); err != nil {
		return errors.Wrap(err, "compress bootstrap tar to tar.gz")
	}
	if err := gzWriter.Close(); err != nil {
		return errors.Wrap(err, "close gzip writer")
	}

	ra, err := local.OpenReader(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrap(err, "open reader for upper blob")
	}
	defer ra.Close()

	blobIDs := []string{}
	for _, blobDigest := range blobDigests {
		blobIDs = append(blobIDs, blobDigest.Hex())
	}
	blobIDsBytes, err := json.Marshal(blobIDs)
	if err != nil {
		return errors.Wrap(err, "marshal blob ids")
	}

	commitBlobs := []string{}
	for idx := range mountBlobs {
		mountBlob := mountBlobs[idx]
		commitBlobs = append(commitBlobs, mountBlob.Desc.Digest.String())
	}
	commitBlobs = append(commitBlobs, upperBlob.Desc.Digest.String())

	bootstrapDesc := ocispec.Descriptor{
		Digest:    digester.Digest(),
		Size:      ra.Size(),
		MediaType: ocispec.MediaTypeImageLayerGzip,
		Annotations: map[string]string{
			converter.LayerAnnotationFSVersion:      "5",
			converter.LayerAnnotationNydusBootstrap: "true",
			layerAnnotationNydusCommitBlobs:         strings.Join(commitBlobs, ","),
		},
	}
	if wf.be.External() {
		bootstrapDesc.Annotations[layerAnnotationNydusBlobIDs] = string(blobIDsBytes)
	}

	bootstrapRc, err := os.Open(bootstrapTarGzPath)
	if err != nil {
		return errors.Wrapf(err, "open bootstrap %s", bootstrapTarGzPath)
	}
	defer bootstrapRc.Close()
	if err := remoter.Push(ctx, bootstrapDesc, true, bootstrapRc); err != nil {
		return errors.Wrap(err, "push bootstrap layer")
	}

	// Push image manifest
	layers := lowerBlobLayers
	for idx := range mountBlobs {
		mountBlob := mountBlobs[idx]
		layers = append(layers, mountBlob.Desc)
	}
	layers = append(layers, upperBlob.Desc)
	layers = append(layers, bootstrapDesc)

	nydusImage.Manifest.Config = *configDesc
	if wf.be.External() {
		nydusImage.Manifest.Layers = []ocispec.Descriptor{bootstrapDesc}
	} else {
		nydusImage.Manifest.Layers = layers
	}

	manifestBytes, manifestDesc, err := wf.makeDesc(ctx, nydusImage.Manifest, nydusImage.Desc)
	if err != nil {
		return errors.Wrap(err, "make config desc")
	}
	if err := remoter.Push(ctx, *manifestDesc, false, bytes.NewReader(manifestBytes)); err != nil {
		return errors.Wrap(err, "push image manifest")
	}

	return nil
}

func (wf *Workflow) Destory() error {
	return errors.Wrap(os.RemoveAll(wf.workDir), "clean up work dir")
}

func prepareMounts(containerMounts []container.Mount, targetPaths []string) ([]mount.Mount, error) {
	targetMounts := []mount.Mount{}

	findMount := func(mounts []container.Mount, targetPath string) *container.Mount {
		var matched *container.Mount
		for idx, mount := range mounts {
			dest := mount.Destination
			if strings.HasPrefix(targetPath, dest) {
				if matched == nil || len(matched.Destination) <= len(dest) {
					matched = &mounts[idx]
				}
			}
		}
		return matched
	}

	for _, targetPath := range targetPaths {
		if !filepath.IsAbs(targetPath) {
			return nil, fmt.Errorf("not a absolute path: %s", targetPath)
		}

		logrus.Infof("for target: %s", targetPath)

		sourceMount := findMount(containerMounts, targetPath)
		if sourceMount == nil {
			return nil, fmt.Errorf("not found mount path: %s", targetPath)
		}
		logrus.Infof("\tcontainer: %s -> %s", sourceMount.Source, sourceMount.Destination)

		hostBase, err := filepath.Rel(sourceMount.Destination, targetPath)
		if err != nil {
			return nil, errors.Wrapf(err, "get rel path for %s", targetPath)
		}
		hostPath := filepath.Join(sourceMount.Source, hostBase)
		target := strings.TrimLeft(targetPath, "/")
		logrus.Infof("\tmount: %s -> %s", hostPath, target)

		targetMounts = append(targetMounts, mount.Mount{
			Type:   "bind",
			Source: hostPath,
			Target: target,
			Options: []string{
				"ro",
				"rbind",
			},
		})
	}

	return targetMounts, nil
}

func (wf *Workflow) commitMountByNSEnter(ctx context.Context, containerPid int, sourceDir, name string) (*digest.Digest, error) {
	logrus.Infof("committing mount: %s", sourceDir)
	start := time.Now()

	blobPath := filepath.Join(wf.workDir, name)
	blob, err := os.Create(blobPath)
	if err != nil {
		return nil, errors.Wrap(err, "create mount blob file")
	}
	defer blob.Close()

	digester := digest.SHA256.Digester()
	counter := Counter{}
	tarWc, err := converter.Pack(ctx, io.MultiWriter(blob, &counter, digester.Hash()), converter.PackOption{
		WorkDir:     wf.workDir,
		FsVersion:   "5",
		Compressor:  "lz4_block",
		BuilderPath: wf.cfg.Base.Builder,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize pack to blob")
	}

	if err := copyFromContainer(ctx, containerPid, sourceDir, tarWc); err != nil {
		return nil, errors.Wrapf(err, "copy %s from pid %d", sourceDir, containerPid)
	}

	if err := tarWc.Close(); err != nil {
		return nil, errors.Wrap(err, "pack to blob")
	}

	mountBlobDigest := digester.Digest()

	logrus.Infof("committed mount: %s, size: %s, elapsed %s", sourceDir, humanize.Bytes(uint64(counter.Size())), time.Since(start))

	return &mountBlobDigest, nil
}

//nolint:unused
func (wf *Workflow) commitMountByBindMount(ctx context.Context, containerMounts []container.Mount, targetPaths []string) (*digest.Digest, error) {
	bindPath, err := os.MkdirTemp(wf.workDir, "mount-")
	if err != nil {
		return nil, errors.Wrap(err, "create temp dir")
	}
	absBindPath, err := filepath.Abs(bindPath)
	if err != nil {
		return nil, errors.Wrapf(err, "get abs path of %s", bindPath)
	}

	targetMounts, err := prepareMounts(containerMounts, targetPaths)
	if err != nil {
		return nil, errors.Wrap(err, "prepare target mounts")
	}

	for _, targetMount := range targetMounts {
		if _, err := os.Stat(targetMount.Source); err != nil {
			return nil, errors.Wrapf(err, "check host path: %s", targetMount.Source)
		}
		target := filepath.Join(absBindPath, targetMount.Target)
		if err := os.MkdirAll(target, 0755); err != nil {
			return nil, errors.Wrapf(err, "prepare target path %s", target)
		}
		defer mount.Unmount(target, 0) //nolint:errcheck
	}

	logrus.Infof("\tbinding mounts to %s", absBindPath)
	if err := mount.All(targetMounts, absBindPath); err != nil {
		return nil, errors.Wrapf(err, "bind mounts to %s", absBindPath)
	}

	blobPath := filepath.Join(wf.workDir, "blob-mount-by-bind")
	blob, err := os.Create(blobPath)
	if err != nil {
		return nil, errors.Wrap(err, "create mount blob file")
	}
	defer blob.Close()

	logrus.Infof("\tpacking mount directory")
	digester := digest.SHA256.Digester()
	tarWc, err := converter.Pack(ctx, io.MultiWriter(blob, digester.Hash()), converter.PackOption{
		WorkDir:     wf.workDir,
		FsVersion:   "5",
		Compressor:  "lz4_block",
		BuilderPath: wf.cfg.Base.Builder,
	})
	if err != nil {
		return nil, errors.Wrap(err, "initialize pack to blob")
	}

	if err := archive.WriteDiff(ctx, tarWc, "", bindPath); err != nil {
		return nil, errors.Wrapf(err, "write diff for path: %s", bindPath)
	}

	if err := tarWc.Close(); err != nil {
		return nil, errors.Wrap(err, "pack to blob")
	}

	mountBlobDigest := digester.Digest()

	return &mountBlobDigest, nil
}

func (wf *Workflow) pause(ctx context.Context, containerIDWithType string, handle func() error) error {
	logrus.Infof("pausing container: %s", containerIDWithType)
	if err := wf.cm.Pause(ctx, containerIDWithType); err != nil {
		return errors.Wrap(err, "pause container")
	}

	if err := handle(); err != nil {
		logrus.Infof("unpausing container: %s", containerIDWithType)
		if err := wf.cm.UnPause(ctx, containerIDWithType); err != nil {
			logrus.Errorf("unpause container: %s", containerIDWithType)
		}
		return err
	}

	logrus.Infof("unpausing container: %s", containerIDWithType)
	return wf.cm.UnPause(ctx, containerIDWithType)
}

func withRetry(handle func() error, total int) error {
	for {
		total--
		err := handle()
		if err == nil {
			return nil
		}

		if total > 0 {
			logrus.WithError(err).Warnf("retry (remain %d times)", total)
			continue
		}

		return err
	}
}

type MountList struct {
	mutex sync.Mutex
	paths []string
}

func NewMountList() *MountList {
	return &MountList{
		paths: make([]string, 0),
	}
}

func (ml *MountList) Add(path string) {
	ml.mutex.Lock()
	defer ml.mutex.Unlock()

	ml.paths = append(ml.paths, path)
}

func (wf *Workflow) Commit(ctx context.Context, opt CommitOption) error {
	logrus.Infof("current envs:")
	logrus.Infof("\thostname: %s", os.Getenv("HOSTNAME"))
	logrus.Infof("\tpod name: %s", os.Getenv("ALIPAY_POD_NAME"))

	targetRef, err := distribution.AppendNydusSuffix(opt.TargetRef)
	if err != nil {
		return errors.Wrap(err, "parse target image name")
	}

	inspect, err := wf.cm.Inspect(ctx, opt.ContainerIDWithType)
	if err != nil {
		return errors.Wrap(err, "inspect container")
	}
	logrus.Infof("inspected container %s:", opt.ContainerIDWithType)
	logrus.Infof("\timage: %s", inspect.Image)
	logrus.Infof("\tpid: %d", inspect.Pid)

	logrus.Infof("pulling base bootstrap")
	start := time.Now()
	image, committedLayers, err := wf.pullBootstrap(ctx, inspect.Image, "bootstrap-base")
	if err != nil {
		return errors.Wrap(err, "pull base bootstrap")
	}
	logrus.Infof("pulled base bootstrap, elapsed: %s", time.Since(start))

	if committedLayers >= opt.MaximumTimes {
		return fmt.Errorf("reached maximum committed times %d", opt.MaximumTimes)
	}

	mountList := NewMountList()

	var upperBlob *Blob
	mountBlobs := make([]Blob, len(opt.WithPaths))
	commit := func() error {
		eg := errgroup.Group{}
		eg.Go(func() error {
			var upperBlobDigest *digest.Digest
			if err := withRetry(func() error {
				upperBlobDigest, err = wf.commitUpperByDiff(ctx, mountList.Add, opt.WithPaths, opt.WithoutPaths, inspect.LowerDirs, inspect.UpperDir, "blob-upper")
				return err
			}, 3); err != nil {
				return errors.Wrap(err, "commit upper")
			}
			logrus.Infof("pushing blob for upper")
			start := time.Now()
			upperBlobDesc, err := wf.pushBlob(ctx, "blob-upper", *upperBlobDigest, opt.TargetRef)
			if err != nil {
				return errors.Wrap(err, "push upper blob")
			}
			upperBlob = &Blob{
				Name: "blob-upper",
				Desc: *upperBlobDesc,
			}
			logrus.Infof("pushed blob for upper, elapsed: %s", time.Since(start))
			return nil
		})

		if len(opt.WithPaths) > 0 {
			for idx := range opt.WithPaths {
				func(idx int) {
					eg.Go(func() error {
						withPath := opt.WithPaths[idx]
						name := fmt.Sprintf("blob-mount-%d", idx)
						var mountBlobDigest *digest.Digest
						if err := withRetry(func() error {
							mountBlobDigest, err = wf.commitMountByNSEnter(ctx, inspect.Pid, withPath, name)
							return err
						}, 3); err != nil {
							return errors.Wrap(err, "commit mount")
						}
						logrus.Infof("pushing blob for mount")
						start := time.Now()
						mountBlobDesc, err := wf.pushBlob(ctx, name, *mountBlobDigest, opt.TargetRef)
						if err != nil {
							return errors.Wrap(err, "push mount blob")
						}
						mountBlobs[idx] = Blob{
							Name: name,
							Desc: *mountBlobDesc,
						}
						logrus.Infof("pushed blob for mount, elapsed: %s", time.Since(start))
						return nil
					})
				}(idx)
			}
		}

		if err := eg.Wait(); err != nil {
			return err
		}

		appendedEg := errgroup.Group{}
		appendedMutex := sync.Mutex{}
		if len(mountList.paths) > 0 {
			logrus.Infof("need commit appened mount path: %s", strings.Join(mountList.paths, ", "))
		}
		for idx := range mountList.paths {
			func(idx int) {
				appendedEg.Go(func() error {
					mountPath := mountList.paths[idx]
					name := fmt.Sprintf("blob-appended-mount-%d", idx)
					var mountBlobDigest *digest.Digest
					if err := withRetry(func() error {
						mountBlobDigest, err = wf.commitMountByNSEnter(ctx, inspect.Pid, mountPath, name)
						return err
					}, 3); err != nil {
						return errors.Wrap(err, "commit appended mount")
					}
					logrus.Infof("pushing blob for appended mount")
					start := time.Now()
					mountBlobDesc, err := wf.pushBlob(ctx, name, *mountBlobDigest, opt.TargetRef)
					if err != nil {
						return errors.Wrap(err, "push appended mount blob")
					}
					appendedMutex.Lock()
					mountBlobs = append(mountBlobs, Blob{
						Name: name,
						Desc: *mountBlobDesc,
					})
					appendedMutex.Unlock()
					logrus.Infof("pushed blob for appended mount, elapsed: %s", time.Since(start))
					return nil
				})
			}(idx)
		}

		return appendedEg.Wait()
	}

	if opt.PauseContainer {
		if err := wf.pause(ctx, opt.ContainerIDWithType, commit); err != nil {
			return errors.Wrap(err, "pause container to commit")
		}
	} else {
		if err := commit(); err != nil {
			return err
		}
	}

	logrus.Infof("merging base and upper bootstraps")
	blobDigests, bootstrapDiffID, err := wf.mergeBootstrap(ctx, *upperBlob, mountBlobs, "bootstrap-base", "bootstrap-merged.tar")
	if err != nil {
		return errors.Wrap(err, "merge bootstrap")
	}

	logrus.Infof("pushing committed image to %s", targetRef)
	if err := wf.pushManifest(ctx, *image, *bootstrapDiffID, targetRef, "bootstrap-merged.tar", blobDigests, upperBlob, mountBlobs); err != nil {
		return errors.Wrap(err, "push manifest")
	}

	return nil
}
