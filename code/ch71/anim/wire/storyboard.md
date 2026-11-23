# What a listener on the wire reads

Claim: spark.authenticate alone proves who talks, but the RPC bytes stay readable. With spark.network.crypto.enabled the same message is ciphertext.
Data: `code/ch71/output/auth.txt` (the secret-no-crypto run: 7707 bytes, app name visible, 21 "spark." strings, the printable strings; the secret-aes-gcm run: 13713 bytes, 0 "spark." strings). Clip times are not to scale.

| beat | start | what moves | caption |
|---|---|---|---|
| plain | 0.5 | driver sends RegisterApplication through the proxy, strings appear | With spark.authenticate only, the proxy reads the message in clear text. |
| strings | 3.2 | class names and -D options listed | It sees class names, the app name and the -D options. |
| aes | 6.4 | same message, bytes are noise | With spark.network.crypto.enabled, the same message is noise. |
| count | 9.0 | counters 21 vs 0 | 21 "spark." strings in clear text, 0 with AES/GCM. |
