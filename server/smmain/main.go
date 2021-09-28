package smmain

func Main() {
	if err := startSM(); err != nil {
		panic(err)
	}
}
