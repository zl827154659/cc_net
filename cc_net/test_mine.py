from cc_net import mine

config = mine.Config()._replace(
    dump="2020-24",
    num_shards=1,
    num_segments_per_shard=1,
)
outputs = mine.mine(config)
print(outputs)
