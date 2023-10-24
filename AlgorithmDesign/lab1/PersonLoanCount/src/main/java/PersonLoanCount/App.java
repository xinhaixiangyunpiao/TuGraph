package PersonLoanCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.antgroup.geaflow.env.Environment;
import com.antgroup.geaflow.env.EnvironmentFactory;
import com.antgroup.geaflow.pipeline.IPipelineResult;
import com.antgroup.geaflow.pipeline.Pipeline;
import com.antgroup.geaflow.pipeline.PipelineFactory;
import com.antgroup.geaflow.pipeline.task.PipelineTask;
import com.antgroup.geaflow.common.config.Configuration;
import com.antgroup.geaflow.example.function.FileSink;
import com.antgroup.geaflow.example.function.FileSource;
import com.antgroup.geaflow.example.config.ExampleConfigKeys;
import com.antgroup.geaflow.api.pdata.stream.window.PWindowSource;
import com.antgroup.geaflow.api.window.impl.AllWindow;
import com.antgroup.geaflow.model.graph.vertex.IVertex;
import com.antgroup.geaflow.model.graph.vertex.impl.ValueVertex;
import java.lang.Integer;
import java.lang.String;
import java.util.*;


/**
 * PersonLoanCount
 */
public class App 
{
    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    public static final String RESULT_FILE_PATH = "/home/blazarx/文档/TuGraph/data/result/result1.csv";

    public static void main( String[] args )
    {
        Environment environment = EnvironmentFactory.onLocalEnvironment();
        IPipelineResult<?> result = submit(environment);
        result.get();
        environment.shutdown();
    }

    public static IPipelineResult<?> submit(Environment environment) {
        Pipeline pipeline = PipelineFactory.buildPipeline(environment);
        Configuration envConfig = environment.getEnvironmentContext().getConfig();
        envConfig.put(FileSink.OUTPUT_DIR, RESULT_FILE_PATH);

        pipeline.submit((PipelineTask) pipelineTaskCxt -> {
            Configuration conf = pipelineTaskCxt.getConfig();

            PWindowSource<IVertex<Integer, String>> prVertices =  
            pipelineTaskCxt.buildSource(new FileSource<>("/home/blazarx/文档/TuGraph/data/sf1/snapshot/Person.csv",
            line -> {
                String[] fields = line.split("|");
                IVertex<Integer, String> vertex = new ValueVertex<>(Integer.valueOf(fields[0]), String.valueOf(fields[1]));
                return Collections.singletonList(vertex);
            }), AllWindow.getInstance()) .withParallelism(conf.getInteger(ExampleConfigKeys.SOURCE_PARALLELISM));

            
        }); 
        return pipeline.execute();
    }
}
