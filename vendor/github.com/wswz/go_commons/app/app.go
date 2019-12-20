package app

import (
	"flag"
	"log"
	"runtime"

	"github.com/wswz/go_commons/utils"
)

var CPU_NUM int

func init() {
	flag.IntVar(&CPU_NUM, "cpunum", 1, "Indicate the number of CPU")
}

func ParseFlag() {
	if !flag.Parsed() {
		flag.Parse()
	}
	runtime.GOMAXPROCS(CPU_NUM)
}

func Run(appName string, initFunc, jobFunc, cleanupFunc func() error) {
	log.Printf("Initial [%s]", appName)
	if err := initFunc(); err != nil {
		log.Printf("Initial [%s] failure: [%s]", appName, err)
		panic(err)
	}
	log.Printf("Initial [%s] complete", appName)
	go func() {
		if err := jobFunc(); err != nil {
			log.Printf("[%s] run error: [%v]", appName, err)
			panic(err)
		}
	}()

	utils.WaitForExitSign()
	log.Printf("[%s] watched the exit signal, start to clean", appName)
	if err := cleanupFunc(); err != nil {
		log.Printf("[%s] clean failed: [%v]", appName, err)
		panic(err)
	}
	log.Printf("[%s] clean complete, exited", appName)
}

func Funcs(funcs ...func() error) func() error {
	return func() error {
		for _, fun := range funcs {
			if err := fun(); err != nil {
				return err
			}
		}
		return nil
	}
}
func LogWrapper(msg string, fun func() error) func() error {
	return func() error {
		log.Println(msg + " start")
		if err := fun(); err != nil {
			log.Printf("%s failed: %v", msg, err)
			return err
		}
		log.Println(msg + " success")
		return nil
	}
}
