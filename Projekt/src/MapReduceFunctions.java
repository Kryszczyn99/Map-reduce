import java.io.IOException;

public class MapReduceFunctions {

    //ZLICZANIE SLOW W PLIKU TEKSTOWYM

    public static void mapFunction(String k, String v) throws IOException {

        String[] splittedWords = v.split("[^A-Za-z0-9]+");

        for(String word:splittedWords) {
            MasterConnection.emit(word,"1");
        }
    }
    //k-> keys (nazwa) //v -> values, tablica 1
    public static void reduceFunction(String k, String[] v) throws IOException {

        int count = 0;
        for(int i = 0; i < v.length; i++){
            count += Integer.parseInt(v[i]);
        }
        MasterConnection.emit(k,String.valueOf(count));
    }


    //ZLICZANIE WARTOSCI ASCII
    /*
    public void mapFunction(String k,String v) throws IOException {

        String[] splittedWords = v.split("[^A-Za-z0-9]+");

        for(String word:splittedWords) {
            int count = 0;
            for(int i = 0; i < word.length(); i++){
                count += (int)word.charAt(i);
            }
            emit(word,String.valueOf(count));
        }
    }

    public void reduceFunction(String k, String[] v) throws IOException {

        int count = 0;
        for(int i = 0; i < v.length; i++){
            count += Integer.parseInt(v[i]);
        }
        emit(k,String.valueOf(count));
    }
    */
    //STARE FUNKCJE BEZ EMITA
        /*
    public void mapFunction(String k,String v) throws IOException {

        HashMap<String, String> mapDatas = new HashMap<String, String>();
        String[] splittedWords = v.split("[^A-Za-z0-9]+");

        File f = new File(path_for_result_after_job);
        if(!f.exists()) {
            f.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_result_after_job));

        for(String word:splittedWords) {
            mapDatas.put(word, "1");
            JSONObject json = new JSONObject(mapDatas);
            writer.write(json.toString());
            writer.write("\n");
            mapDatas.clear();
        }

        writer.close();
    }

     */
    /*
    public void reduceFunction(String k, String v) throws IOException {
        HashMap<String, String> mapDatas = new HashMap<String, String>();
        JSONObject jObject  = new JSONObject(v);
        Iterator<String> keys= jObject.keys();
        while (keys.hasNext())
        {
            int count = 0;
            String keyValue = (String)keys.next();
            JSONArray datas = jObject.getJSONArray(keyValue);
            for(int i = 0; i < datas.length(); i++){
                count += Integer.parseInt(datas.getString(i));
            }
            mapDatas.put(keyValue,String.valueOf(count));
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path_for_result_after_job));
        JSONObject json = new JSONObject(mapDatas);
        writer.write(json.toString());
        writer.close();
        System.out.println(mapDatas);
    }

     */
}
