package main

import (
	"kvraft"
	"fmt"
	"flag"
	"bufio"
	"os"
	"strings"
)

func main() {
	numServersPtr := flag.Int("servers", 3, "The Number of KV Servers")
	flag.Parse()

	fmt.Printf("Got %d servers\n", *numServersPtr)

	cfg := kvraft.MakeConfig(nil, "Run", *numServersPtr, 0)
	client := cfg.MakeClient()
	fmt.Printf("Possible Commands: Get:<Key>, Put:<Key>:<Value>, Append:<Key>:<Value>\n")
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf("Enter a command...\n")
	for scanner.Scan() {
		cmd := scanner.Text()
		ops := strings.Split(cmd, ":")

		if len(ops) == 2 && ops[0] == "Get" {
			fmt.Printf("Executing command, %s\n", cmd)
			retVal := client.Get(ops[1])
			fmt.Printf("Got Value: %s\n", retVal)
		} else if len(ops) == 3 && (ops[0] == "Put" || ops[0] == "Append") {
			if (ops[0] == "Put"){
				fmt.Printf("Executing command, %s\n", cmd)
				client.Put(ops[1], ops[2])
				//fmt.Printf("Got Value: %s\n", retVal)	
			} else if (ops[0] == "Append") {
				fmt.Printf("Executing command, %s\n", cmd)
				client.Append(ops[1], ops[2])
				//fmt.Printf("Got Value: %s\n", retVal)	
			}
		} else {
			fmt.Printf("Invalid Command %s\n", cmd)
			fmt.Printf("Possible Commands: Get:<Key>, Put:<Key>:<Value>, Append:<Key>:<Value>\n")
		}
		fmt.Printf("Enter a command...\n")
	}
	
}