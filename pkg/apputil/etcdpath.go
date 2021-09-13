package apputil

import "fmt"

func etcdPathAppPrefix(service string) string {
	return fmt.Sprintf("/bd/app/%s", service)
}

func EtcdPathAppContainerIdHb(service, id string) string {
	return fmt.Sprintf("%s/containerhb/%s", etcdPathAppPrefix(service), id)
}
