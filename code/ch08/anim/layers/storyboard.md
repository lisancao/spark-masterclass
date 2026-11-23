# ch08-anim-layers

Claim: each configuration source sits on top of the one below it, and the effective value is the top one that is set.
Numbers: code/ch08/output/layers.txt (sections 1 and 2).

| id | start | what moves | on-screen text | why |
|---|---|---|---|---|
| open | 0.0 | title, empty stack, value 200 | Where does a config value come from? | hook |
| file | 1.2 | spark-defaults.conf box enters, value 200 -> 11 | A config file overrides the built-in default | first layer |
| conf | 3.4 | --conf box enters, value -> 12 | A --conf flag overrides the file | second layer |
| builder | 5.6 | builder.config box enters, value -> 13 | The builder overrides the flags | third layer |
| set | 7.8 | spark.conf.set then SET enter, value -> 14 -> 15 | At runtime, the last change wins | runtime layer |
| reset | 10.6 | the two runtime boxes leave, value -> 13 | RESET goes back to the value at session start | the surprise |
| unset | 12.8 | value -> 200 | unset goes back to the built-in default | second surprise |
