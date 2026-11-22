"""Chapter 77, Step 10: the public record of the case: the JIRA fields of SPARK-60206 and its neighbours,
read from the Apache JIRA REST API, and the state of the pull request.

Run from code/:  uv run python ch77/jira.py      (needs network; the output is a dated snapshot)
"""
import datetime, json, urllib.request

def issue(key):
    url = (f"https://issues.apache.org/jira/rest/api/2/issue/{key}"
           "?fields=summary,issuetype,status,priority,versions,fixVersions,components,labels,created")
    with urllib.request.urlopen(url, timeout=30) as r:
        return json.load(r)

print(f"snapshot taken {datetime.date.today()}\n")
for key in ["SPARK-60206", "SPARK-54119"]:
    d = issue(key)
    f = d["fields"]
    print(f"{key}: {f['summary']}")
    print(f"  type {f['issuetype']['name']}, priority {f['priority']['name']}, status {f['status']['name']}, "
          f"created {f['created'][:10]}")
    print(f"  affects {[v['name'] for v in f['versions']]}, fix {[v['name'] for v in f['fixVersions']]}, "
          f"components {[c['name'] for c in f['components']]}, labels {f['labels']}\n")

req = urllib.request.Request("https://api.github.com/repos/apache/spark/pulls/59416",
                             headers={"Accept": "application/vnd.github+json", "User-Agent": "book-example"})
with urllib.request.urlopen(req, timeout=30) as r:
    pr = json.load(r)
print(f"apache/spark#59416: {pr['title']}")
print(f"  state {pr['state']}, merged {pr['merged']}, {pr['changed_files']} files, "
      f"+{pr['additions']} -{pr['deletions']}, base {pr['base']['ref']}")
