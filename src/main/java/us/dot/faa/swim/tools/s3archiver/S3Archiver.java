package us.dot.faa.swim.tools.s3archiver;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.codec.binary.Hex;

import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.SelectObjectContentArgs;
import io.minio.SelectResponseStream;
import io.minio.UploadObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.CompressionType;
import io.minio.messages.InputSerialization;
import io.minio.messages.Item;
import io.minio.messages.JsonType;
import io.minio.messages.OutputSerialization;

public class S3Archiver implements Runnable, AutoCloseable {
    final Logger logger = LoggerFactory.getLogger(S3Archiver.class);
    final MinioClient minioClient;
    final DateFormat archiveFileDateFormat;
    final DateFormat minioArchiveDatePrefixFormat;
    final String bucket;
    final CronDefinition cronDefinition = CronDefinitionBuilder.defineCron().withSeconds().and().withMinutes().and()
            .withHours().and().withDayOfMonth().and().instance();
    final CronParser parser = new CronParser(cronDefinition);

    final Thread minioArchiverThead;
    final ExecutorService archiveUploadExecuterService;
    final CountDownLatch latch = new CountDownLatch(1);

    static boolean uploadInProcess = false;
    static ZonedDateTime lastTimeReference = ZonedDateTime.now();

    private boolean prefixDateTimeFirst;
    private Path archiveTempFilePath;
    private Path archiveTempPath;
    private Path pendingArchivesToUploadPath;
    private ArrayList<String> propertiesToInclude = new ArrayList<String>();
    private DocumentBuilderFactory factory;
    private DocumentBuilder builder;
    private HashMap<String, XPathExpression> xpaths = new HashMap<String, XPathExpression>();
    private ExecutionTime executionTime;
    private boolean asJson;
    private boolean compressArchive = true;
    private String categoryPrefix;
    private int maxPendingUploads = 4;

    private BufferedWriter archiveFileWriter = null;
    private boolean firstRecord = false;

    public S3Archiver(String bucketName, String s3Url, String s3Region, String s3AccessKey, String s3AccessSecret,
            String archiveCategoryPrefix, Boolean useDatePrefixFirst)
            throws IOException, ParserConfigurationException, XPathExpressionException {

        factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        builder = factory.newDocumentBuilder();

        bucket = bucketName;
        categoryPrefix = archiveCategoryPrefix;
        prefixDateTimeFirst = useDatePrefixFirst;

        archiveFileDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HHmmss'Z'");
        archiveFileDateFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));

        minioArchiveDatePrefixFormat = new SimpleDateFormat("yyyy/MM/dd/");
        minioArchiveDatePrefixFormat.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));

        minioClient = MinioClient.builder().region(s3Region).endpoint(s3Url).credentials(s3AccessKey, s3AccessSecret)
                .build();

        minioArchiverThead = new Thread(this);
        archiveUploadExecuterService = Executors.newSingleThreadExecutor();

        minioArchiverThead.setName("MinioArchiver-Thread");

        executionTime = ExecutionTime.forCron(parser.parse("0 0 * *"));

        archiveTempPath = Paths.get("archive/" + categoryPrefix);
        pendingArchivesToUploadPath = Paths.get(archiveTempPath + "/pendingUpload");
        Files.createDirectories(pendingArchivesToUploadPath);

        archiveTempFilePath = Paths.get(archiveTempPath + "/archive.temp");

    }

    public S3Archiver setArchiveAsJson(boolean archiveAsJson) {
        this.asJson = archiveAsJson;
        return this;
    }

    public S3Archiver setUploadChronScheule(String chronSchedule) {
        executionTime = ExecutionTime.forCron(parser.parse(chronSchedule));
        return this;
    }

    public S3Archiver setCompression(boolean compress) {
        this.compressArchive = compress;
        return this;
    }

    public S3Archiver setMaxPendingUploads(int max) {
        this.maxPendingUploads = max;
        return this;
    }

    public int getMaxPendingUploads() {
        return this.maxPendingUploads;
    }

    public S3Archiver setPropertiesToArchive(String[] propertiesToArchive) {
        for (String property : propertiesToArchive) {
            if (property != null && !property.trim().isEmpty()) {
                propertiesToInclude.add(property);
            }
        }
        return this;
    }

    public S3Archiver addXpath(String property, String xpathString) throws XPathExpressionException {

        XPathFactory xpathfactory = XPathFactory.newInstance();
        XPath xpath = xpathfactory.newXPath();
        xpaths.put(property, xpath.compile(xpathString));

        return this;
    }

    public void archiveMessage(String message, HashMap<String, String> properties) throws Exception {

        try {
            if (getPendingUploadCount() > maxPendingUploads) {
                throw new Exception("Message archiving suspeneded due to max pending uploads of " + maxPendingUploads
                        + " has been reached.");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            while (getPendingUploadCount() > maxPendingUploads) {
                Thread.sleep(1000);
            }
        }

        properties.put("archivedTimestamp", Long.toString(Instant.now().toEpochMilli()));

        final ZonedDateTime lastExecutionTime = executionTime.lastExecution(lastTimeReference).get();
        final ZonedDateTime nextExecutionTime = executionTime.nextExecution(lastTimeReference).get();
        final long nanosToNextExecution = Duration.between(ZonedDateTime.now(), nextExecutionTime).toNanos();

        if (nanosToNextExecution <= 0) {

            synchronized (lastTimeReference) {
                lastTimeReference = ZonedDateTime.now();
            }

            try {
                if (archiveFileWriter != null) {
                    archiveFileWriter.close();
                }
            } catch (IOException e) {
                logger.warn("Failed to close archiveFileWriter", e);
            }

            firstRecord = true;
            moveArchiveTempFileForUplaod(lastExecutionTime, nextExecutionTime);
            createNewArchivedFileWriter(archiveTempFilePath);
        } else if (archiveFileWriter == null) {
            if (!Files.exists(archiveTempFilePath)) {
                firstRecord = true;
            }
            createNewArchivedFileWriter(archiveTempFilePath);
        }

        try {
            if (firstRecord) {
                firstRecord = false;
            } else {
                archiveFileWriter.newLine();
            }

            final JSONObject messageToArchive = new JSONObject();
            final HashMap<String, String> archiveMessageProperties = new HashMap<String, String>();

            if (propertiesToInclude.size() > 0) {
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    if (propertiesToInclude.contains(property.getKey())) {
                        archiveMessageProperties.put(property.getKey(), property.getValue());
                    }
                }
                messageToArchive.put("properties", new JSONObject(archiveMessageProperties));
            } else {
                for (Entry<String, String> prop : properties.entrySet()) {
                    archiveMessageProperties.put(prop.getKey(), prop.getValue());
                }
            }

            if (xpaths.size() > 0) {
                InputStream stream = new ByteArrayInputStream(
                        message.getBytes(Charset.forName("UTF-8")));
                Document doc = builder.parse(stream);
                for (Map.Entry<String, XPathExpression> entry : xpaths.entrySet()) {
                    archiveMessageProperties.put(entry.getKey(),
                            (String) entry.getValue().evaluate(doc, XPathConstants.STRING));
                }
            }

            messageToArchive.put("properties", new JSONObject(archiveMessageProperties));

            if (message != null) {
                if (asJson) {
                    messageToArchive.put("message", XML.toJSONObject(message));
                } else {
                    messageToArchive.put("message",
                            Hex.encodeHexString(message.getBytes(StandardCharsets.UTF_8)));
                }
            }

            archiveFileWriter.write(messageToArchive.toString());
            archiveFileWriter.flush();
        } catch (IOException | SAXException | XPathExpressionException e) {
            logger.error("Failed to write to swim archive file", e);
        }
    }

    private void moveArchiveTempFileForUplaod(ZonedDateTime from, ZonedDateTime to) {

        final Date archiveFileDateFrom = Date.from(from.toInstant());
        final Date archiveFileDateTo = Date.from(to.toInstant());
        final Path fileToUpload = Paths.get(minioArchiveDatePrefixFormat.format(archiveFileDateFrom).replace("/", "_")
                + categoryPrefix.replace("/", "_") + "_" + archiveFileDateFormat.format(archiveFileDateFrom) + "_"
                + archiveFileDateFormat.format(archiveFileDateTo) + ".ndjson");

        try {
            Path filePendingUpload = pendingArchivesToUploadPath.resolve(fileToUpload);
            Files.move(archiveTempFilePath, filePendingUpload);
            uploadArchive(filePendingUpload);
        } catch (IOException e) {
            logger.error("Failed to move file (" + fileToUpload + ") to upload directory", e);
        }
    }

    public void uploadArchive(Path filePathToArchive) {

        logger.info("Uploading " + filePathToArchive);

        archiveUploadExecuterService.submit(new Runnable() {

            @Override
            public void run() {
                String archiveFileName = filePathToArchive.getFileName().toString();

                if (!archiveFileName.endsWith("gz") && compressArchive) {
                    if (compressArchiveFile(filePathToArchive)) {
                        try {
                            archiveFileName = archiveFileName + ".gz";
                            Files.delete(filePathToArchive);
                        } catch (IOException e) {
                            logger.error("Failed to delete file: " + filePathToArchive.toString(), e);
                        }
                    }
                }

                final String fileToArchivePath = filePathToArchive.getParent() + "/" + archiveFileName;
                try {

                    if (!categoryPrefix.endsWith("/")) {
                        categoryPrefix += "/";
                    }
                    archiveFileName = archiveFileName.replace("_", "/");
                    final String minioArchiveDatePrefix = archiveFileName.split(categoryPrefix)[0];
                    final String minioArchiveFilename = archiveFileName.replace(minioArchiveDatePrefix, "").replace("/",
                            "_");

                    final String minioArchiveObject;

                    if (prefixDateTimeFirst) {
                        minioArchiveObject = minioArchiveDatePrefix + categoryPrefix
                                + minioArchiveFilename;
                    } else {
                        minioArchiveObject = categoryPrefix + minioArchiveDatePrefix
                                + minioArchiveFilename;
                    }

                    String[] fileNameSplit = fileToArchivePath.split("_");
                    String beginTime = fileNameSplit[fileNameSplit.length - 2];
                    String endTime = fileNameSplit[fileNameSplit.length - 1].split("\\.")[0];

                    Map<String, String> userMetadata = new HashMap<>();
                    userMetadata.put("beginTime", beginTime);
                    userMetadata.put("endTime", endTime);

                    minioClient.uploadObject(UploadObjectArgs.builder().bucket(bucket).object(minioArchiveObject)
                            .filename(fileToArchivePath).userMetadata(userMetadata).build());

                    try {
                        Files.delete(Paths.get(fileToArchivePath));
                    } catch (IOException e) {
                        logger.error("Failed to remove: " + Paths.get(fileToArchivePath).getFileName().toString(), e);
                    }

                    logger.info("Uploaded " + fileToArchivePath + " to minio as " + minioArchiveObject);
                } catch (IOException | InvalidKeyException | ErrorResponseException | InsufficientDataException
                        | InternalException | InvalidResponseException | NoSuchAlgorithmException | ServerException
                        | XmlParserException | IllegalArgumentException e) {
                    logger.error("Failed to archive file, retrying: " + fileToArchivePath, e);
                    e.printStackTrace();

                    try {
                        Thread.sleep(30000);
                    } catch (InterruptedException e1) {
                        // do nothing
                    }

                    if (archiveFileName.endsWith(".gz")) {
                        uploadArchive(Paths.get(filePathToArchive.toAbsolutePath() + ".gz"));
                    } else {
                        uploadArchive(filePathToArchive);
                    }
                }
            }

        });

    }

    private boolean compressArchiveFile(Path fileToCompress) {

        boolean success = false;

        try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(fileToCompress.toString() + ".gz"))) {
            try (FileInputStream in = new FileInputStream(fileToCompress.toString())) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
            success = true;
        } catch (IOException e) {
            logger.warn("Failed to compress before archive", e);

            try {
                Files.delete(Paths.get(fileToCompress.toString() + ".gz"));
            } catch (IOException e1) {
                logger.error("Failed to delete file: " + fileToCompress.toString() + ".gz", e1);
            }
        }

        return success;
    }

    private void createNewArchivedFileWriter(Path currentFilePath) {
        try {
            if (archiveFileWriter != null) {
                archiveFileWriter.close();
            }
            archiveFileWriter = new BufferedWriter(new FileWriter(currentFilePath.toString(), true));
        } catch (IOException e) {
            logger.error("Failed to create archiveFileWriter", e);
        }
    }

    public long getPendingUploadCount() throws IOException {
        try (Stream<Path> files = Files.list(pendingArchivesToUploadPath)) {
            return files.count();
        }
    }

    public void getMessagesFromArchive(String archiveCategoryPrefix, Instant from, Instant to,
    Map<String, List<String>> properties, boolean useOrSelect, MessageReciever reciever) {

        Instant refTime = from;

        while (refTime.isBefore(to.plus(1, ChronoUnit.DAYS))) {

            String fromPrefix;
            if (prefixDateTimeFirst) {
                fromPrefix = minioArchiveDatePrefixFormat.format(Date.from(refTime)) + archiveCategoryPrefix;
            } else {
                fromPrefix = archiveCategoryPrefix + minioArchiveDatePrefixFormat.format(Date.from(refTime));
            }

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder().bucket(bucket).prefix(fromPrefix).includeUserMetadata(true)
                            .recursive(true).build());

            refTime = refTime.plus(1, ChronoUnit.DAYS);
            List<Item> itemList = new ArrayList<>();

            results.forEach(item -> {
                try {
                    Item file = item.get();
                    final String beginTimeString = file.userMetadata().get("X-Amz-Meta-Begintime");
                    final String endTimeString = file.userMetadata().get("X-Amz-Meta-Endtime");

                    final Date beginTimeDate = archiveFileDateFormat.parse(beginTimeString);
                    final Date endTimeDate = archiveFileDateFormat.parse(endTimeString);

                    final Instant beginTime = beginTimeDate.toInstant();
                    final Instant endTime = endTimeDate.toInstant();

                    if (beginTime.isAfter(from) && endTime.isBefore(to)) {

                        itemList.add(file);
                    }

                } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                        | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                        | IllegalArgumentException | IOException | ParseException e) {

                    logger.error(e.getMessage(), e);
                }
            });

            itemList.parallelStream().forEach(fileItem -> {
                try (SelectResponseStream responseStream = getMessages(fileItem, properties,
                        useOrSelect)) {

                    try (BufferedReader reader = new BufferedReader(
                            new InputStreamReader(new BufferedInputStream(responseStream)))) {

                        String message = "";
                        while ((message = reader.readLine()) != null) {
                            ArchivedMessageObject archivedMessageObject = new ArchivedMessageObject(message);

                            Instant messageime = Instant
                                    .ofEpochMilli(Long
                                            .parseLong(archivedMessageObject.getPropertyValue("archivedTimestamp")));

                            if (messageime.isAfter(from) && messageime.isBefore(to)) {
                                reciever.recieveMessage(archivedMessageObject);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
        }

    }

    private SelectResponseStream getMessages(Item fileItem,
            Map<String, List<String>> properties, boolean useOrSelect)
            throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException,
            InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException,
            IllegalArgumentException, IOException {
        // build expression
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("select * from S3Object as o");

        if (properties != null && properties.size() > 0) {
            queryBuilder.append(" where");
            int i = 1;
            for (Map.Entry<String, List<String>> prop : properties.entrySet()) {
                queryBuilder.append(" (");
                int ii = 1;
                for (String value : prop.getValue()) {
                    queryBuilder.append(" o.properties." + prop.getKey() + "='" + value + "'");
                    if (ii< prop.getValue().size()) {
                        if (useOrSelect) {
                            queryBuilder.append(" OR");
                        } else {
                            queryBuilder.append(" AND");
                        }
                    }
                    ii++;
                }
                queryBuilder.append(")");
                if (i < properties.size()) {
                    if (useOrSelect) {
                        queryBuilder.append(" OR");
                    } else {
                        queryBuilder.append(" AND");
                    }
                }
               
            }
        }


        SelectResponseStream stream = null;
        try {

            CompressionType compressionType = CompressionType.NONE;
            if (fileItem.userMetadata().get("content-type").equals("application/x-gzip")) {
                compressionType = CompressionType.GZIP;
            }

            InputSerialization is = new InputSerialization(compressionType, JsonType.LINES);
            OutputSerialization os = new OutputSerialization(null);

            String query = queryBuilder.toString();

            stream = minioClient
                    .selectObjectContent(
                            SelectObjectContentArgs.builder().bucket(bucket)
                                    .object(fileItem.objectName()).sqlExpression(query)
                                    .inputSerialization(is).outputSerialization(os).requestProgress(true).build());

            return stream;
        } catch (Exception e) {
            if (stream != null) {
                stream.close();
            }
            throw e;
        }

    }

    public void start() throws IOException {

        try {
            try (Stream<Path> walk = Files.walk(pendingArchivesToUploadPath)) {
                for (Path path : walk.filter(Files::isRegularFile).collect(Collectors.toList())) {
                    uploadArchive(path);
                }
            }

            if (Files.exists(archiveTempFilePath)) {
                final FileTime fileLastModifiedTime = Files.getLastModifiedTime(archiveTempFilePath);
                if (fileLastModifiedTime.compareTo(FileTime.from(Instant.now())) <= 0) {
                    final ZonedDateTime zonnedFileTime = ZonedDateTime.ofInstant(fileLastModifiedTime.toInstant(),
                            ZoneId.of("UTC"));

                    final ZonedDateTime lastExecutionTime = executionTime.lastExecution(zonnedFileTime).get();
                    final ZonedDateTime nextExecutionTime = executionTime.nextExecution(zonnedFileTime).get();
                    moveArchiveTempFileForUplaod(lastExecutionTime, nextExecutionTime);
                }
            }

            minioArchiverThead.start();
        } catch (IOException e) {
            throw e;
        }
    }

    @Override
    public void close() {
        try {
            archiveFileWriter.close();
        } catch (IOException e) {
            logger.warn("Failed to close archiveFileWriter", e);
        } finally {
            archiveUploadExecuterService.shutdown();
            latch.countDown();
        }
    }

    @Override
    public void run() {
        try {
            synchronized (latch) {
                latch.await();
                logger.info("S3 Archiver closed");
            }
        } catch (InterruptedException e) {
            logger.error("Interupped", e);
        }
    }

    public String getCategoryPrefix() {
        return this.categoryPrefix;
    }
}
