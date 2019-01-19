package my.conf;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import my.conf.ObjectMapper.Property;

public class Config
{
  public static Config from(final Path file) throws IOException { return new Config(file).init(); }
  public static Config from(final Properties properties) throws IOException { return new Config(properties).init(); }

  public static Map<String, String> parseArgs(String[] args)
  { // parse cmdline arguments to map, argumnets are in following format:
    //   $java My.MainClass key=value key2=value2 key3 key4=value41,value42,value43
    // then you would configure keys with @Property for validation and loading.
    return Arrays.stream(args).map(x -> x.split("\\s*=\\s*", 2))
        .collect(Collectors.toMap(a -> a[0].trim(),
            a -> a.length > 1 ? a[1].trim() : /*flag=*/ "true" ));
  }   

  @Property(name="consumers"                                                        ) public int        nconsumers;
  @Property(name="kafka.consumer.config"                                            ) public String     kafkaConsumerConfig;
  @Property(name="topics"                                                           ) public Pattern    topics;
  @Property(name="writer.workqueue"                                                 ) public int        writerWorkQueueSize;
  @Property(name="writers"                       , defaultValue="4"                 ) public int        nwriters;
  @Property(name="writer.queue.poll.timeout.ms"  , defaultValue="120000"            ) public int        writerQueuePollTimeoutMillis;
  @Property(name="writer.outdir"                 , isDirectory=true, isWritable=true) public Path       outdir;
  @Property(name="writer.hardlinks"              , isDirectory=true, isWritable=true) public List<Path> hardlinkDirs;
  @Property(name="file.name.prefix"                                                 ) public String     fileNamePrefix;
  @Property(name="csv.bufsize"                   , defaultValue="65536"             ) public int        csvbufsize;
  @Property(name="gzip.bufsize"                  , defaultValue="65536"             ) public int        gzipbufsize;
  @Property(name="consumer.offset"               , defaultValue="seektoend"         ) public String     consumerOffset;
  @Property(name="metrics.period.seconds"        , defaultValue="60"                ) public int        metricsPeriodSecs;
  @Property(name="enable.writer.queue.metrics"   , defaultValue="false"             ) public boolean    enableWriterQueueMetrics;
  @Property(name="metadata.queue.size"           , defaultValue="65536"             ) public int        metadataQueueSize;
  @Property(name="metadata.queue.poll.timeout.ms", defaultValue="120000"            ) public int        metadataQueuePollTimeoutMillis;

  public Properties kafkaprops; // we load this in postInit
  public String[]   events;     // event.*=n

  public final Optional<Path> configfile;
  public final Properties     properties;

  Config(final Path file) throws IOException
  {
    configfile = Optional.of(file);
    properties = new Properties();
    try (Reader r = Files.newBufferedReader(configfile.get())) { properties.load(r); }
    properties.putAll(System.getProperties()); // system props overrides file props
  }

  Config(final Properties props) throws IOException
  {
    configfile = Optional.empty();
    properties = (Properties) props.clone(); // make our own copy
    properties.putAll(System.getProperties()); // system props overrides file props
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  Config init() throws IOException
  {
    final Map<String, String> hm = (Map) properties;
    new ObjectMapper() {
      @Override
      protected FieldHandler getFieldHandler(final Field field) {
        return field.getName().equals("hardlinkDirs") ? ObjectMapper.lisOfPathFieldHandler : null;
      };
    }.mapPublicFields(this, hm);

    // load kafka properties
    if (configfile.isPresent())
    {
      kafkaprops = new Properties();
      try (Reader r = Files.newBufferedReader(configfile.get().getParent().resolve(kafkaConsumerConfig))) { kafkaprops.load(r); }
    }
    else
    {
      System.err.println("configfile not provided, not loading kafkaprops");
    }

    // load events array
    final Map<Integer, String> eventsmap = new HashMap<>();
    properties.stringPropertyNames().forEach(key -> {
      if (key.startsWith("event."))
        eventsmap.put(Integer.parseInt(properties.getProperty(key)), key.substring("event.".length()));
    });
    events = new String[Collections.max(eventsmap.keySet()) + 1]; // eventsid's starts from 1
    eventsmap.forEach((k, v) -> { events[k] = v; });

    return this;
  }

  public void forEachField(final BiConsumer<String, Object> consumer)
  {
    for (final Field field : getClass().getFields())
    {
      if (!Modifier.isStatic(field.getModifiers()))
      {
        try
        {
          final String name = field.getName();
          final Object val = field.get(this);
          final Class<? extends Object> cls = val.getClass();
          if (cls.isArray())
          {
            final int len = Array.getLength(val);
            final Object[] arr = new Object[len];
            for (int i = 0; i < arr.length; i++)
              arr[i] = Array.get(val, i);
            consumer.accept(name, Arrays.asList(arr));
          }
          else
          {
            consumer.accept(name, val);
          }
        }
        catch (final Throwable e) { e.printStackTrace(); }
      }
    }
  }

  // public Config log()
  // {
  //   forEachField((k, v) -> { log.info("{}: {}", k, v); });
  //   return this;
  // }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    forEachField((k, v) -> {
      if (sb.length() > 0) sb.append(' ');
      sb.append(k).append('=').append(v);
    });
    return sb.toString();
  }
}
