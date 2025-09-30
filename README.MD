## What is this?
I built this as a learning project for myself. I wanted to learn how databases like Redis work under the hood, and I've found the best way to learn these kinds of things is to literally build them yourself from scratch. So, here's my little version of the Redis database.

I also made an entire playlist where I basically teach others how I built this project. You can find that here: \
https://youtube.com/playlist?list=PLTGiYd8gFivgrd_INfVrFDRuBBHfiTxRP&si=Eqxlcfu2g7bjnSIs

## Features
I guess I should explain more about what this thing actually does:
- RESP serialization and deserialization (obviously 😅)
- 17 Redis commands implemented 
```
COMMAND, GET, SET, DEL, EXISTS, KEYS, SAVE, BGSAVE, FLUSHDB, DBSIZE, AUTH, EXPIRE, TTL, BGREWRITEAOF, MULTI, EXEC, DISCARD
```
- Database Transactions with `MULTI`
- RDB data persistence, both automated and manual, with SHA256 checksum protection.
- AOF data persistence, with compact rewriting features.
- LRU and LFU eviction policies implemented for all keys.
- Concurrency-safe (this is less a flex and more a standard in Go, tbh)
- All features configurable with a simple Redis-compatible config file.

## What can you do with this?
Whatever you want to, tbh. Like I said, I mainly just built this for learning purposes, and to just make some content for my YT channel. 

Feel free to clone this repo, play around with it, or do whatever you like with it. Just don't go around pretending you made it yourself or trying to sell it or something. Some people actually are that weird. Just don't be weird, but otherwise have fun. 😊

## About Me
I'm Hassan. I just enjoy building cool things in VSCode. You can find me here:
- YouTube: https://www.youtube.com/@itshassanaziz
- X/Twitter: https://x.com/nothassanaziz
- Discord: @itshassanaziz

I'm always open to opportunities, so feel free to DM. 🙌
