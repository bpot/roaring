package roaring

type Snapshot struct {
	keys       []uint16
	containers []container
}
