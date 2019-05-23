public class StringFormatTest {
    public static void main(String[] args) {
        String f = "%s:%s:%s";
        String[] a = new String[]{"123|", "abc|", "134"};
        System.err.println(String.format(f, a));

    }
}
