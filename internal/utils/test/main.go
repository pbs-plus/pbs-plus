package main

import (
	"log"

	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func main() {
	drives, err := utils.GetLocalDrives()
	if err != nil {
		log.Fatalln(err)
	}

	for _, drive := range drives {
		log.Println(drive.Letter)
		log.Println(drive.Type)
		log.Println("====")
	}
}
