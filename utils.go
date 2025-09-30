package main

func contains(slice []string, item string) bool {
	for _, i := range slice {
		if item == i {
			return true
		}
	}
	return false
}
