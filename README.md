# FAA SWIM S3 Archiver

Provides a tool to archive data in S3, and query data that is archived through S3 Select. Data is archived in json newline delimited files with two json object; properties and message. Properties provide metadata about the message that can be used with S3 Select to receive a specified subset of message from the archive. The message itself is stored HEX encoded and is decoded back to its original form when using getMessagesFromArchive() function. 

## Installing

  1. Clone this repository
  2. Run mvn clean install
  3. Add dependency to applicable project pom

```xml
	<dependency>
		<groupId>us.dot.faa.swim.tools</groupId>
		<artifactId>swim-s3-archiver</artifactId>
		<version>1.0</version>
	</dependency>
```

## Usage

Import Required Dependancies

```java
	import us.dot.faa.swim.tools.s3archiver.S3Archiver;
	import us.dot.faa.swim.tools.s3archiver.ArchivedMessageObject;
	import us.dot.faa.swim.tools.s3archiver.MessageReciever;	
```

Create new S3Archiver

```java
	 S3Archiver s3Archiver = new S3Archiver("BUCKET", "S3_SERVICE_URL", "S3_REGION",
                "S3_KEY", "S3_SECRET", "ARCHIVE_PREFIX", false);
```

Set Upload Schedule (chron [Sec Min Hour Day])

```java
	s3Archiver.setUploadChronScheule("0 0/15 * *");
```

Start S3 Archiver

```java
	 s3Archiver.start();
	
```

Add Data for Archiving
	

```java
	HashMap<String, String> properties = new HashMap<String, String>();
	properties.put("propertyName", propertyValue);
	s3Archiver.archiveMessage("Data to Archive", properties);
```

Query Archive for Specific Data in Archive Files

```java
	Hashtable<String,List<String>> filters = new Hashtable<>();	
 	List<String> values = new ArrayList<String>();
	values.add("Property_Value");
	filters.put("Property_Key", values);
	s3Archiver.getMessagesFromArchive(s3Archiver.getCategoryPrefix(), Instant.now().minusSeconds(10 * 60 * 60),
    Instant.now(), filters, true, this);
```

Recieve and Process Messages

```java
	@Override
    public void recieveMessage(ArchivedMessageObject archivedMessage) {
        System.out.println("Properties:");
		System.out.println(archivedMessage.getProperties().toString());
		System.out.println("Message:");

    }
```