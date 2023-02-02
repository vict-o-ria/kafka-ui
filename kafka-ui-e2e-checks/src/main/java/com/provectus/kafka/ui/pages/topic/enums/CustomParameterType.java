package com.provectus.kafka.ui.pages.topic.enums;

public enum CustomParameterType {
  COMPRESSION_TYPE("compression.type", "producer"),
  DELETE_RETENTION_MS("delete.retention.ms", "86400000"),
  FILE_DELETE_DELAY_MS("file.delete.delay.ms", "60000"),
  FLUSH_MESSAGES("flush.messages", "9223372036854775807"),
  FLUSH_MS("flush.ms", "9223372036854775807"),
  FOLLOWER_REPLICATION_THROTTLED_REPLICAS("follower.replication.throttled.replicas", ""),
  INDEX_INTERVAL_BYTES("index.interval.bytes", "4096"),
  LEADER_REPLICATION_THROTTLED_REPLICAS("leader.replication.throttled.replicas", ""),
  MAX_COMPACTION_LAG_MS("max.compaction.lag.ms", "9223372036854775807"),
  MESSAGE_DOWNCONVERSION_ENABLE("message.downconversion.enable", "true"),
  MESSAGE_FORMAT_VERSION("message.format.version", "2.3-IV1"),
  MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS("message.timestamp.difference.max.ms", "9223372036854775807"),
  MESSAGE_TIMESTAMP_TYPE("message.timestamp.type", "CreateTime"),
  MIN_CLEANABLE_DIRTY_RATIO("min.cleanable.dirty.ratio", "0.5"),
  MIN_COMPACTION_LAG_MS("min.compaction.lag.ms", "0"),
  PREALLOCATE("preallocate", "false"),
  RETENTION_BYTES("retention.bytes", "-1"),
  SEGMENT_BYTES("segment.bytes", "1073741824"),
  SEGMENT_INDEX_BYTES("segment.index.bytes", "10485760"),
  SEGMENT_JITTER_MS("segment.jitter.ms", "0"),
  SEGMENT_MS("segment.ms", "604800000"),
  UNCLEAN_LEADER_ELECTION_ENABLE("unclean.leader.election.enable", "0");

  private final String customParameter;
  private final String value;

  CustomParameterType(String customParameter, String value) {
    this.customParameter = customParameter;
    this.value = value;
  }

  public String getCustomParameter() {
    return customParameter;
  }

  public String getValue(){
    return value;
  }

}
