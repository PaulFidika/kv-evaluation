Garnet (Redis) Results:

- Using a 'watch' (optimistic lock), it takes 4 - 8.5 seconds to run 1,000 updates on 1 key, with a 95th-percnetile latency of 282ms (for one write). This is because of all the concurrent writes interering with each other.

- If you do not retry from fails (with optimistic locks), then you can do about 169 / 1,000 updates in 450ms. Therefore, Redis' optimistic locking is really only useful in low-contention workloads.

- Using Incrby with a pipeline, you can run 1,000 updates on 3 keys in 285ms - 350ms. Note that the correctness is not perfect; for example if I set the limit to 500, sometimes it'll go up to 503 or 505. Also note that I have to change the data-model from using seiralized structs to using individual keys, otherwise incrby would not work.

- Incrby does not require retrying.

- Lua scirpts are not recommended due to poor performance, and Garnet disables them by default.

- The SetNX lock system works better than 'watch', as you would expect. It takes 3.5 seconds to to do 1000 reads + sets on a single key.

- These tests were done running on localhost; running remotely might be much slower due to all the retries; this only applies for the lock-acquiring however.

- Redis methods: (1) Watch with Multi/exec (optimistic lock). (2) Incrby (atomic increments), (3) Lua scripting (server side execution), (4) SetNX (acquire a lock).

---

Olric Results:

- I could not get the locking to work. Olric does not support the notion of 'watch' (optimistic locking) like Redis does. Without locking our behavior was incorrect; i.e., out of 1,000 updates only about 799 would get through. Idk why acquiring a lock just froze the thread forever.

- Instead, we can do atomic increments or decrements. Using this alone we could do 1k updates correctly at 13ms. Adding a get command in (to check the limit) increased time to 129ms. Latency was in microseconds. This is partially because we were using an in-memory rate-limiter in the same program, whereas Garnet was using a localhost API.

- (Look for other ways to do updates?)

---

OpenMeter Results:

- When you check availability (get usage), it is woefully out of date; the time it takes from (write) -> (read your write) is at least 15 seconds. This is probably due to the Kafka ingest queue.

- Insert latency is fast; about 25ms on average

- Availability (check) latency is slow, about 56ms - 100ms. This is probably because the analytic queries it's running (on clickhouse) are slow.

- If you have 3 counters, you have to do 3 separate checks for access

- This library is not even remotely appropriate for in-line rate limiting.

- UI doesn't support refresh intervals less than a day.

- Doesn't support limiting concurrency or have the notion of 'completed requests'.

---

TiKV Results:

- TiKV is hard to use because you need two containers (PD and TiKV itself) in order to run it. I also couldn't get the playground to work. I couldn't install the python client. The golang client could be installed but would not connect to the playground running on my same machine.


