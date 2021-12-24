public class MapperTest {
    public static void main(String[] args) {
        String str = "1363157984040   13602846565 5C-0E-8B-8B-B6-00:CMCC  120.197.40.4    2052.flash2-http.qq.com 综合门户    15  12  1938    2910    200";
        String[] split = str.split("(\\s)+"); // 以正则 \s 匹配空白
        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];
        System.out.println(phone);
        System.out.println(upFlow);
        System.out.println(downFlow);
    }
}
