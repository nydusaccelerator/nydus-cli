### Nydus CLI

#### Nydus Commit

``` shell
./nydus-cli --config ./smoke/tests/texture/config.registry.yml commit \
--docker.addr /var/run/docker.sock \
--container docker://$CONTAINER_ID \
--target localhost:5000/nginx:nydus-committed \
--with-mount-path /my-mount"
```
