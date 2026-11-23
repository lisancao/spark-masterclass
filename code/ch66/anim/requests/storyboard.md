# Requests from a paginated API source

Claim: planning sends one request for the total, each action re-reads every page, a 503 is retried in the reader, and only cached rows avoid requests.
Data: `data.json`, request logs from `code/ch66/output/api_source.txt`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| plan | 0.4 | p0 from the planning worker | Planning asks for page 0 to get the total. | planning has a cost |
| tasks | 3.0 | five requests | Five tasks request their pages at the same time. | parallel pages |
| retry | 4.4 | p3 503, retry | Page 3 fails with 503. The reader tries again after a pause. | retries in the reader |
| count | 6.2 | 7 requests | The first count() sent seven requests: one to plan, six to read. | the total |
| again | 8.6 | six requests | A second action plans nothing new, but it reads all pages again. | no row cache |
| cache | 11.0 | none | Only cached rows answer a query without a request. | the fix |
