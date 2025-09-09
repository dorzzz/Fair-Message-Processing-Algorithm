There are two main purposes for this README file:

1. To describe the policy implemented for the FMPA and the required tests.

2. To demonstrate the use of NATS for sending and receiving messages concurrently for each tenant. An `instruction.txt` file has been added for this purpose.


I started by breaking down the requirements and reading up on Go and NATS since both were new to me. That gave me a clearer picture of how the message flow and consumers work.

The idea was to keep things simple with a hash map for tracking tenants and their quotas, and then decide fairly who can process at any moment. Tenants with more processing sets naturally get more share, but no one is fully blocked.

Messages are shuffled in batches to avoid bias, and a semaphore enforces the global capacity. Deferred ones get retried before going back to JetStream. This way the system stays fair, efficient, and responsive to bursts.



used chatGPT.
