# Animation: the listener bus copies each event into a queue per consumer, and a full queue drops events

**Claim.** LiveListenerBus posts each event to named AsyncEventQueues with their own threads; a slow listener fills its queue, and Spark then drops events and counts them in numDroppedEvents (Chapter 61, "Where the numbers come from").

**Numbers.** None measured. The mechanism and the default capacity (10000) come from the chapter's source reading (`LiveListenerBus.scala`). Slots, event count and speed are schematic and say so on screen.

**Backend:** hf. **Themes:** paper. **Formats:** landscape. **Camera cut:** no.

## Beats

| beat | start | what moves | on-screen text | VO (draft) | why this beat |
|---|---|---|---|---|---|
| `open` | 0.0 | scheduler, three queues, consumers | The listener bus: one queue per consumer (stays) | Every number in the UI starts as an event. | subject at frame 0 |
| `fanout` | 0.8 | events copy into all queues; two drain | Each event goes to every queue, and each queue has its own thread | Each event goes to every queue. | the fan-out |
| `fill` | 5.0 | the shared queue fills | A slow listener fills its queue | A slow listener fills its queue. | the cause |
| `drop` | 8.6 | events fall off, counter rises | A full queue drops events, and the metric counts them (stays) | Then Spark drops events, and counts them. | the effect |
