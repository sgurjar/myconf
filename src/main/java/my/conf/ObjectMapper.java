package my.conf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/** maps properties to an object fields */
public class ObjectMapper
{
  /** annotation used on object to map public fields with keys in the properties */
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Property
  {
    String  name        () default ""  ;
    String  defaultValue() default ""  ;
    String  pattern     () default ""  ;
    String  delimiter   () default "," ;

    boolean required    () default true;

    // int value attributes
    int     min         () default                 0;
    int     max         () default Integer.MAX_VALUE;

    // Path value attributes
    boolean isDirectory () default false;
    boolean isFile      () default false;
    boolean isReadable  () default false;
    boolean isWritable  () default false;
  }

  /** validate the value and set the value on field based on class type of the field */
  interface FieldHandler
  {
    void set(Object obj, final Field field, final Property annotation, final String name, final String value) throws IllegalAccessException;
  }

  public static final FieldHandler lisOfPathFieldHandler = (obj, field, annotation, name, value) -> {
    final ArrayList<Path> alist = new ArrayList<>();
    for (String s : value.split(Pattern.quote(annotation.delimiter())))
      if (!(s = s.trim()).isEmpty())
        alist.add(getPath(annotation, name, s));
    field.set(obj, alist);
  };

  static Path getPath(final Property annotation, final String name, final String value)
  {
    final Path path = Paths.get(value);
    if (annotation.isDirectory() && !Files.isDirectory(path  )) throw new RuntimeException("Not a directory. "         + name + "=" + path);
    if (annotation.isFile()      && !Files.isRegularFile(path)) throw new RuntimeException("Not a file. "              + name + "=" + path);
    if (annotation.isReadable()  && !Files.isReadable(path   )) throw new RuntimeException("Read permission denied. "  + name + "=" + path);
    if (annotation.isWritable()  && !Files.isWritable(path   )) throw new RuntimeException("Write permission denied. " + name + "=" + path);
    return path;
  }

  /** handles field of String type */
  private final FieldHandler strField = (obj, field, annotation, name, value) -> {
    final String pattern = trimToNull(annotation.pattern(), null);
    if (pattern != null && !Pattern.matches(pattern, value))
      throw new RuntimeException("Pattern '" + pattern + "' mismatch " + name + "=" + value);
    field.set(obj, value);
  };

  /** handles field of int type */
  private final FieldHandler intField = (obj, field, annotation, name, value) -> {
    try
    {
      final int min = annotation.min(), max = annotation.max(), nval = Integer.parseInt(value);
      if (nval < min) throw new RuntimeException("Invalid property '" + name + "' min=" + min + " value=" + nval);
      if (nval > max) throw new RuntimeException("Invalid property '" + name + "' max=" + max + " value=" + nval);
      field.setInt(obj, nval);
    }
    catch (final NumberFormatException e)
    {
      throw new RuntimeException("Invalid property '" + name + "' expected a number, got '" + value + "'");
    }
  };

  /** handles field of boolean type */
  private final FieldHandler booleanField = (obj, field, annotation, name, value) -> {
    field.setBoolean(obj, Boolean.parseBoolean(value));
  };

  /** handles field of java.util.List type
   *  only List<String> is supported
   */
  private final FieldHandler listField = (obj, field, annotation, name, value) -> {
    final ArrayList<String> alist = new ArrayList<>();
    for (String s : value.split(Pattern.quote(annotation.delimiter())))
      if (!(s = s.trim()).isEmpty())
        alist.add(s);
    field.set(obj, alist);
  };

  /** handles fields of java.nio.file.Path type */
  private final FieldHandler pathField = (obj, field, annotation, name, value) -> {
     field.set(obj, getPath(annotation, name, value));
  };

  private final FieldHandler patternField = (obj, field, annotation, name, value) -> {
    final Pattern pat = Pattern.compile(value);
    field.set(obj, pat);
  };

  // parallel lists
  private final List<Predicate<Field>> predicates    = new ArrayList<>();
  private final List<FieldHandler>     fieldHandlers = new ArrayList<>();

  public ObjectMapper()
  {
    assign(intField    , field -> field.getType().isAssignableFrom(int.class    ));
    assign(booleanField, field -> field.getType().isAssignableFrom(boolean.class));
    assign(strField    , field -> field.getType().isAssignableFrom(String.class ));
    assign(pathField   , field -> field.getType().isAssignableFrom(Path.class   ));
    assign(listField   , field -> field.getType().isAssignableFrom(List.class   ));
    assign(patternField, field -> field.getType().isAssignableFrom(Pattern.class));
  }

  void assign(final FieldHandler fh, final Predicate<Field> pred)
  {
    fieldHandlers.add(fh);
    predicates.add(pred);
  }

  /** maps instance public fields to properties */
  public <T> T mapPublicFields(final T instance, final Map<String, String> properties)
  {
    // this only returns public instance/static fields
    // and we will set value to them if they have "Property" annotation on them.
    final Field[] fields = instance.getClass().getFields();
    for (final Field field : fields)
    {
        set(instance, field, properties);
    }
    return instance;
  }

  // helper
  private void set(final Object instance, final Field field, final Map<String, String> properties)
  {
    final Property annotation = field.getAnnotation(Property.class);

    if (annotation == null)
      return; // skip

    final String name  = trimToNull(annotation.name(), field.getName());
    final String value = trimToNull(properties.get(name), annotation.defaultValue());

    if (value == null)
    {
      if (annotation.required())
        throw new RuntimeException("Required property is missing '" + name + "'");
      else
        return; // optional and null is OK
    }

    try
    {
      getFieldHandlerInternal(field).set(instance, field, annotation, name, substVars(value, properties));
    }
    catch (final IllegalAccessException e)
    {
      throw new RuntimeException(name + ", " + field + ", " + e, e);
    }
  }

  // let derive classes pass their own handler
  protected FieldHandler getFieldHandler(final Field field) {return null;}

  // field type to field handler map
  private FieldHandler getFieldHandlerInternal(final Field field)
  {
    // let derived classes pass its own field handler
    final FieldHandler fh = getFieldHandler(field);
    if (fh != null)
      return fh;

    // o/w do your thing
    for (int i = 0; i < predicates.size(); i++)
    {
      if (predicates.get(i).test(field))
        return fieldHandlers.get(i);
    }

    throw new RuntimeException("No field handler for '" + field + "'");
  }

  /** substitute ${var} in the value */
  public static String substVars(final String val, final Map<String, String> props) throws IllegalArgumentException
  { // @courtesy org.apache.log4j.helpers.OptionConverter.substVars(String, Properties)
    return substVars_NoInfiniteRecursion(val, props, 0);
  }

  static String DELIM_START     = "${";
  static char   DELIM_STOP      = '}';
  static int    DELIM_START_LEN = 2;
  static int    DELIM_STOP_LEN  = 1;

  private static String substVars_NoInfiniteRecursion(
                          final String              val,
                          final Map<String, String> props,
                          int                       preventInfiniteRecursion)
  {
    if (preventInfiniteRecursion > 100)
      throw new IllegalArgumentException(val + " variable substitution consist a cycle, breaking recursive calls to avoid infinite recursion");

    final StringBuffer sbuf = new StringBuffer();

    int i = 0;
    int j, k;

    while (true)
    {
      j = val.indexOf(DELIM_START, i);
      if (j == -1)
      {
        // no more variables
        if (i == 0)
        { // this is a simple string
          return val;
        }
        else
        { // add the tail string which contains no variables and return the result.
          sbuf.append(val.substring(i, val.length()));
          return sbuf.toString();
        }
      }
      else
      {
        sbuf.append(val.substring(i, j));
        k = val.indexOf(DELIM_STOP, j);
        if (k == -1)
        {
          throw new IllegalArgumentException('"' + val + "\" has no closing brace. Opening brace at position " + j + '.');
        }
        else
        {
          j += DELIM_START_LEN;
          final String key = val.substring(j, k);
          // first try in System properties
//          String replacement = getSystemProperty(key, null);
          // then try props parameter
//          if (replacement == null && props != null)
//          {
          final String replacement = props.get(key);
//          }

          if (replacement != null)
          {
            // Do variable substitution on the replacement string
            // such that we can solve "Hello ${x2}" as "Hello p1"
            // the where the properties are
            // x1=p1
            // x2=${x1}
            final String recursiveReplacement = substVars_NoInfiniteRecursion(replacement, props, ++preventInfiniteRecursion);
            sbuf.append(recursiveReplacement);
          }
          i = k + DELIM_STOP_LEN;
        }
      }
    }
  }

  // gotta've dis
  static String trimToNull(String s, final String orElse)
  {
    return s != null && !(s = s.trim()).isEmpty() ? s : orElse != null ? trimToNull(orElse, null) : null;
  }
}
