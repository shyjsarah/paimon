package org.apache.paimon.rest;

import org.apache.paimon.PagedList;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;

import java.util.List;

import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_ID;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_ACCESS_KEY_SECRET;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_OSS_ENDPOINT;
import static org.apache.paimon.rest.RESTCatalogOptions.DLF_REGION;
import static org.apache.paimon.rest.RESTCatalogOptions.TOKEN_PROVIDER;

public class RESTApiPopTest {


    public static void main(String[] args) {

        Options options = new Options();
        options.set(RESTCatalogOptions.URI, "https://dlfnext-pre.cn-hangzhou.aliyuncs.com");
//        options.set(CatalogOptions.WAREHOUSE, "dlf_samples");
        options.set(TOKEN_PROVIDER, "dlf-open-api");
        options.set(DLF_ACCESS_KEY_ID, "");
        options.set(DLF_ACCESS_KEY_SECRET, "");
        options.set(DLF_REGION, "cn-hangzhou");
        options.set(DLF_OSS_ENDPOINT, "oss-cn-hangzhou.aliyuncs.com");

        RESTApi restApi = new RESTApi(options);

        PagedList<String> tables =  restApi.listTablesPaged("system", 10, null, null, null);
        tables.getElements().forEach(System.out::println);
    }
}
