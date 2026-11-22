# Animation: Avro bytes have no types, so a wrong reader schema decodes without an error

**Claim.** The reader schema says what each byte means; reading `total` as `float` takes 4 of the 8 bytes, the next field reads the wrong byte, and the result is a row of wrong values with no error. From Chapter 16, Steps 14 and 15.

**Numbers.** The 14 bytes and both decoded rows from `code/ch16/output/avro.txt`. The four bytes after the union are not read by the wrong schema (derived from the byte layout).

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | title, the 14 bytes | Avro bytes have no types (stays) | Avro bytes have no types. | topic at frame 0 |
| `right` | 1.0 | brackets walk the four fields; values appear | The reader schema says what each byte means | The reader schema says what each byte means. | the correct decode |
| `wrong` | 5.4 | brackets with a float field; values shift | Read total as float: 4 of the 8 bytes, and the rest shifts | Read total as float: 4 of the 8 bytes, and the rest shifts. | the failure |
| `end` | 10.2 | the last 4 bytes dim | No error. A row of wrong values that looks correct. (stays) | No error. A row of wrong values that looks correct. | the danger |
