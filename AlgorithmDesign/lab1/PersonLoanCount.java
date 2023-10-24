public class PersonLoanCount{

	public static void main(String[] args) {
		Environment environment = EnvironmentFactory.onLocalEnvironment();
		IPipelineResult<?> result = submit(environment);
		result.get();
		environment.shutdown();
	}

	public static IPipelineResult<?> submit(Environment environment) {
                Pipeline pipeline = PipelineFactory.buildPipeline(environment);
	}
}
