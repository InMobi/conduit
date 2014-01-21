package com.inmobi.conduit.visualization.server;

public class MessageStats {
  private String topic;
  private Long messages;
  private String hostname;

  public MessageStats(String topic, Long messages, String hostname) {
    this.topic = topic;
    this.messages = messages;
    this.hostname = hostname;
  }

  public MessageStats(MessageStats other) {
    this.topic = other.getTopic();
    this.messages = other.getMessages();
    this.hostname = other.getHostname();
  }

  public String getTopic() {
    return topic;
  }

  public Long getMessages() {
    return messages;
  }

  public void setMessages(Long messages) {
    this.messages = messages;
  }

  public String getHostname() {
    return hostname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MessageStats that = (MessageStats) o;

    if (hostname != null ? !hostname.equals(that.hostname) :
        that.hostname != null) {
      return false;
    }
    if (!topic.equals(that.topic)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = topic.hashCode();
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MessageStats{" +
        "topic='" + topic + '\'' +
        ", messages=" + messages +
        ", hostname='" + hostname + '\'' +
        '}';
  }
}
