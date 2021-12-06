package us.dot.faa.swim.tools.s3archiver;

public interface MessageReciever {
    
    public void recieveMessage(ArchivedMessageObject archivedMessage);
}
