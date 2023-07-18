package ingres

import "testing"

func TestInitOpenAPI (t *testing.T) {
    env, err := InitOpenAPI()
    if err != nil {
        t.Fail()
    }
    ReleaseOpenAPI(env)
}
