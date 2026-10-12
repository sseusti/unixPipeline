package main

import "sync"

func RunPipeline(cmds ...cmd) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	close(in)

	for _, job := range cmds {
		out := make(chan interface{})
		wg.Add(1)
		go func(in, out chan interface{}, job cmd) {
			defer wg.Done()
			defer close(out)

			job(in, out)
		}(in, out, job)

		in = out
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[uint64]struct{})

	for row := range in {
		email := row.(string)

		wg.Add(1)
		go func(email string) {
			defer wg.Done()

			user := GetUser(email)

			mu.Lock()
			_, isSeen := seen[user.ID]
			if !isSeen {
				seen[user.ID] = struct{}{}
			}
			mu.Unlock()

			if !isSeen {
				out <- user
			}
		}(email)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	var wg sync.WaitGroup

	var batch []User

	for user := range in {
		batch = append(batch, user.(User))

		if len(batch) == 2 {
			batchCopy := append([]User(nil), batch...)
			wg.Add(1)
			go func(batch []User) {
				defer wg.Done()

				msgs, _ := GetMessages(batch...)

				for _, msg := range msgs {
					out <- msg
				}

			}(batchCopy)

			batch = batch[:0]
		}
	}

	if len(batch) != 0 {
		batchCopy := append([]User(nil), batch...)
		wg.Add(1)
		go func(batch []User) {
			defer wg.Done()

			msgs, _ := GetMessages(batch...)

			for _, msg := range msgs {
				out <- msg
			}

		}(batchCopy)
	}

	wg.Wait()
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string
}
