# Decode a VARIANT value

Claim: a reader decodes an object from its header, size, field ids (into the metadata dictionary) and offsets (into the data); ids follow the sort of the names, the data keeps the JSON order.
Data: `data.json`, the metadata (27 bytes) and value (44 bytes) of the first event of day 1 in `code/ch61/output/layout.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| bytes | 0.4 | three rows of bytes | The metadata holds each field name one time. | the two binaries |
| header | 1.6 | 02, 04 highlighted | The value starts with a header byte and a size. | the header |
| fields | 4.2 | id, name, offset, data, for each field | Field k: id k names it, offset k points to its data. | the walk |
| end | 10.4 | decoded fields | The ids follow the sort of the names. The data keeps the JSON order. | two orders |
