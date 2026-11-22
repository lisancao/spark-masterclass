# Storyboard: Part IX chapter clips, chapters 47 to 51

Chapter format (1280x720, about 95x24 cells), embedded in the book's pages. The VO lines are drafts, checked against the scripts' output. The clips are muted in the book.

## ch47.tape

| Beat | On screen | VO (draft) | Has to be readable |
|---|---|---|---|
| where | the trace of a Python data source | Spark runs each method of a data source in a Python worker. load() runs schema(). The first action plans the partitions. The tasks run read(). | the driver pid, the other pids, the three sections |
| balance | round robin against largest-first planning | Round robin gives each task the same number of files. Largest-first gives each task the same number of bytes. | both lists of (files, bytes), the bytes per task table |
| errors | when each error appears | Planning errors appear at the first action as PYTHON_DATA_SOURCE_ERROR. Errors in read() appear in a task as PYTHON_EXCEPTION. | each label, `fails at build` or `fails at action`, the condition |
