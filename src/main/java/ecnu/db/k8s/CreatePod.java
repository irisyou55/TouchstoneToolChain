import io.kubernetes.client.Copy;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import io.kubernetes.client.util.exception.CopyNotSupportedException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * @author youshuhong
 * 创建configmap、创建一定数量的pod，并将指定文件上传到pod的指定目录中
 */
public class CreatePod {
    public static void main(String[] args)
            throws IOException, ApiException, InterruptedException, CopyNotSupportedException {
        // ApiClient client = Config.defaultClient();

        String configPath = "config";//加载集群的config文件
        ApiClient client =
                ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(configPath))).build();
        Configuration.setDefaultApiClient(client);
        CoreV1Api api = new CoreV1Api();
        V1PodList list = new V1PodList();
        int pod_num = 1; //pod数量
        //创建configmap
        File file = new File("yaml/kustomization.yaml");
        V1ConfigMap yamlConfigMap = (V1ConfigMap) Yaml.load(file);
        V1ConfigMap createConfigMap = api.createNamespacedConfigMap("default", yamlConfigMap, null, null, null);

        //创建pod里env变量
        V1EnvFromSource envFromSource = new V1EnvFromSource();
        V1ConfigMapEnvSource configMapEnvSource = new V1ConfigMapEnvSource();
        configMapEnvSource.setName(createConfigMap.getMetadata().getName());
        envFromSource.configMapRef(configMapEnvSource);

        String path = System.getProperty("java.class.path");//jar文件本地相对路径

        //创建一定数量的pod
        for (int i = 0; i < pod_num; i++) {
            Map<String, String> pod_map = new HashMap<String, String>();
            pod_map.put("app", "pod-" + i);

            V1Pod pod = new V1PodBuilder()
                    .withNewMetadata()
                    .withName("pod-" + i)
                    .withLabels(pod_map)
                    .endMetadata()
                    .withNewSpec()
                    .addNewContainer()
                    .withName("container-1")
                    .withImage("openjdk")
                    .withImagePullPolicy("IfNotPresent")
                    .withEnvFrom(envFromSource)
                    .addNewVolumeMount()
                    .withName("shared")
                    .withMountPath("/tmp/files")
                    .endVolumeMount()
                    .withCommand("/bin/bash", "-c", "--")
                    .withArgs("while true; do sleep 30; done;")
                    .endContainer()
                    .addNewContainer()
                    .withName("container-2")
                    .withImage("openjdk")
                    .withImagePullPolicy("IfNotPresent")
                    .addNewVolumeMount()
                    .withName("shared")
                    .withMountPath("/var/files")
                    .endVolumeMount()
                    .withCommand("/bin/bash", "-c", "--")
                    .withArgs("java -jar " + path + ";while true; do sleep 30; done")
                    .endContainer()
                    .addNewVolume()
                    .withName("shared")
                    .endVolume()
                    .endSpec()
                    .build();
            api.createNamespacedPod("default", pod, null, null, null);
            list.addItemsItem(pod);
        }

        //等待pod的status更新为Running
        Thread.sleep(3000);
        Copy copy = new Copy();
        //将jar文件传入pod
        for (V1Pod item : list.getItems()) {
            copy.copyFileToPod("default", item.getMetadata().getName(), item.getSpec().getContainers().get(0).getName(), Paths.get("/Users/irisyou/Documents/k8s-pod/" + path), Paths.get("/" + path));
        }
        //断开链接
        client.getHttpClient().connectionPool().evictAll();

    }
}