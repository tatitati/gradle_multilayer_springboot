fun consume(consumer: KafkaConsumer<String, String>){
        // get partitions, lazy poll
        val targetTime: ZonedDateTime? = ZonedDateTime.of(
            2021, 6, 28,
            9, 0, 0, 0,
            ZoneId.systemDefault()
        )
        val epochMillis = targetTime!!.toInstant().toEpochMilli()
        println("targetTime = $targetTime")


        while(consumer.assignment().isEmpty()) {
            consumer.poll(Duration.ofMillis(5))
        }

        // get offset for specific timestamps
        val partitions: MutableList<PartitionInfo> = consumer.partitionsFor(topic)!!
        val partitionsToTimestamp = mutableMapOf<TopicPartition, Long>()
        for(partition in partitions){
            partitionsToTimestamp.put(
                    TopicPartition(partition.topic(), partition.partition()),
                    epochMillis
            )
        }
        val offsets: MutableMap<TopicPartition, OffsetAndTimestamp> = consumer.offsetsForTimes(partitionsToTimestamp)!!


        // reset to offset
        for ((topicPartition, offsetTimestamp) in offsets){
            consumer.seek(topicPartition, offsetTimestamp.offset())
        }


        while(true){
            val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(1))
            records.forEach{ record ->
                val topic: String  = record.topic()
                val msg: String = record.value()
                val offset: Long = record.offset()

                println("$topic, ${Date(record.timestamp())} ---> [$offset]}")
            }
        }
    }

    @Test
    fun run(){
        // this.consume(buildConsumerConfluent())
        this.consume(buildConsumerAws())
    }
