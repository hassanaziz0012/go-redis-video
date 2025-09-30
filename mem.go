package main

type sample struct {
	k string
	v *Item
}

func sampleKeys(state *AppState) []sample {
	maxSamples := state.conf.memSamples
	samples := make([]sample, 0, maxSamples)

	for k, v := range DB.store {
		samples = append(samples, sample{
			k: k,
			v: v,
		})
		if len(samples) >= maxSamples {
			break
		}
	}

	return samples
}
