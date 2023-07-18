package main

import "fmt"
import "github.com/ildus/ingo/ingres"

func main() {
    _, err := ingres.InitOpenAPI()
    if err != nil {
        fmt.Printf("init error: %v\n", err)
    }
}
