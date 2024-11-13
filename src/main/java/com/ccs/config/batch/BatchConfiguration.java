package com.ccs.config.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.validation.BindException;

import com.ccs.model.EmpData;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {
	@Autowired
	StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	JobBuilderFactory jobBuilderFactory;
	
	@Qualifier("CcsDbDataSource")
	@Autowired
	DataSource dataSource;
	
	@Bean 
	public FlatFileItemReader<EmpData> reader() {
		return new FlatFileItemReaderBuilder<EmpData>()
	    .name("csvBuilder")
		.delimited()
        .delimiter(",")
        .names(new String[] {"empNumber", "empName", "empJoinDt", "empAddr"})
        .linesToSkip(1)
        .resource(new FileSystemResource("C:/rajiv/workspace-insursa/springbatch-csv-csv/src/main/resources/csv/employee_lite_in.csv"))
        .fieldSetMapper(new FieldSetMapper<EmpData>() {
			@Override
			public EmpData mapFieldSet(FieldSet fieldSet) throws BindException {
				return new EmpData(fieldSet.readInt("empNumber"),
						fieldSet.readString("empName"),
						fieldSet.readDate("empJoinDt", "yyyy-MM-dd"),
						fieldSet.readString("empAddr"));
			}
        })
        .build();
	}
    
	@Bean
	public JdbcBatchItemWriter<EmpData> writer(DataSource dataSource) {
		BeanPropertyItemSqlParameterSourceProvider<EmpData> provider = new BeanPropertyItemSqlParameterSourceProvider<EmpData>();

		JdbcBatchItemWriter<EmpData> itemWriter = new JdbcBatchItemWriter<EmpData>();

		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("insert into employee_batch(EMP_NAME, EMP_JOIN_DT, EMP_ADDR) values (:empName, :empJoinDt, :empAddr)");
		itemWriter.setItemSqlParameterSourceProvider(provider);
		
		return itemWriter;
	}
	
	@Bean
	public Step executeStep() {
		return stepBuilderFactory.get("executeStep")
				.<EmpData, EmpData>chunk(200)
				.reader(reader())
				.writer(writer(dataSource))
				.build();
	}
	
	@Bean
	public Job processJob() {
		return jobBuilderFactory.get("processJob")
				.incrementer(new RunIdIncrementer())
				.flow(executeStep())
				.end()
				.build();
	}
}
