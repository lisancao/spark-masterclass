# Stage, then publish

Claim: the jsonlog sink stages task files, moves them at commit, replaces the manifest with one rename, deletes old data last, and abort() removes the staging folder.
Data: `data.json`, file names, manifest counts and the abort call from `code/ch82/output/writers.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| stage | 0.4 | three files into staging | An overwrite starts. Each task writes into a staging folder. | nothing visible yet |
| move | 3.6 | files to the output folder | The commit moves the staged files next to the data. | publish step 1 |
| manifest | 5.2 | manifest swaps | One rename replaces the manifest. Readers see old or new. | atomic switch |
| delete | 6.6 | old files go | Only then does the commit delete the old files. | safe order |
| fail | 8.0 | partial files | A failed append leaves partial files in staging. | the failure |
| abort | 10.0 | staging cleared, manifest same | abort() deletes the staging folder. The manifest does not change. | the cleanup |
