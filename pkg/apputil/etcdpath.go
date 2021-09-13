package apputil

import "fmt"

func EtcdPathAppPrefix(service string) string {
	return fmt.Sprintf("/bd/app/%s", service)
}

func EtcdPathAppContainerIdHb(service, id string) string {
	return fmt.Sprintf("%s/containerhb/%s", EtcdPathAppPrefix(service), id)
}

func EtcdPathAppShardHbId(service, id string) string {
	return fmt.Sprintf("%s/shardhb/%s", EtcdPathAppPrefix(service), id)
}
