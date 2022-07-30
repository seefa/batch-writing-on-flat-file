package ir.seefa.batch;

import ir.seefa.mapper.CustomerRowMapper;
import ir.seefa.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Saman Delfani
 * @version 1.0
 * @since 2022-07-30 23:28:26
 */
@Configuration
public class ChuckBasedJob {

    public static String[] names = new String[]{"customerNumber", "customerName", "contactLastName", "contactFirstName", "phone", "addressLine1", "addressLine2", "city", "state", "postalCode", "country", "salesRepEmployeeNumber", "creditLimit"};

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    public ChuckBasedJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }


    @Bean
    public PagingQueryProvider queryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setSelectClause("SELECT customerNumber, customerName, contactLastName, contactFirstName, phone, addressLine1, addressLine2, city, state, postalCode, country, salesRepEmployeeNumber, creditLimit");
        factory.setFromClause("FROM `spring-batch`.customers");
        factory.setSortKey("customerNumber");
        factory.setDataSource(dataSource);
        return factory.getObject();
    }

    @Bean
    public ItemReader<Customer> itemReader() throws Exception {
        return new JdbcPagingItemReaderBuilder<Customer>()
                .dataSource(dataSource)
                .name("jdbcPagingItemReader")
                .queryProvider(queryProvider())
                .pageSize(10)                           // MUST be equal to chunk size
                .rowMapper(new CustomerRowMapper())
                .build();
    }

    @Bean
    public ItemWriter<Customer> itemWriter() {
        FlatFileItemWriter<Customer> itemWriter = new FlatFileItemWriter<>();
        itemWriter.setHeaderCallback(writer -> {
            writer.write("customerNumber,customerName,contactLastName,contactFirstName,phone,addressLine1,addressLine2,city,state,postalCode,country,salesRepEmployeeNumber,creditLimit");
        });
        itemWriter.setResource(new FileSystemResource("/Users/sami/tmp/data/customer_output.csv"));

        DelimitedLineAggregator<Customer> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(names);

        aggregator.setFieldExtractor(fieldExtractor);
        itemWriter.setLineAggregator(aggregator);
        return itemWriter;
    }

    @Bean
    public Step chunkBasedReadingFlatFileStep() throws Exception {
        return this.stepBuilderFactory.get("chunkBasedWritingOnFlatFileStep")
                .<Customer, Customer>chunk(10)                  // Must be equal to queryProvider page size
                .reader(itemReader())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job chuckOrientedJob() throws Exception {
        return this.jobBuilderFactory.get("chunkOrientedWritingOnFlatFileJob")
                .start(chunkBasedReadingFlatFileStep())
                .build();

    }
}
