# FIFO and FAIR on one Connect server

Claim: under FIFO a small query waits for a large job; under FAIR it gets the next free task slot.
Data: `data.json` (schematic schedule: A = 16 tasks of 1.25 s, B = 2 tasks of 0.4 s, 2 slots, B at 2 s);
measured end numbers from `code/ch30/output/contention.txt`.
Render: `anim render . --formats landscape --themes ide`, scaled to 1280x720 into `book/media/clips/ch30-anim-fifo-fair.mp4`.

| beat | start | what moves | caption | why |
|---|---|---|---|---|
| open | 0.0 | title, lanes | Session A starts a long job: sixteen tasks on two slots. | set up the slots |
| arrive | 2.55 | "B arrives" pill | Session B sends a small query two seconds later. | the contention moment |
| split | 3.3 | B tasks wait (FIFO) or start (FAIR) | FIFO keeps every slot for A. FAIR gives B the next free slot. | the mechanism |
| result | 7.05 | cursor ends | Measured: B took 8.1 to 9.6 s under FIFO, 1.7 to 3.3 s under FAIR. | end on measured numbers |
