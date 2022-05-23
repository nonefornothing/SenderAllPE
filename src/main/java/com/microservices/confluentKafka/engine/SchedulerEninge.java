//package com.microservices.confluentKafka.engine;
//
//import java.io.File;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Service;
//
//import com.microservices.confluentKafka.services.StreamService;
//
//@Service
//public class SchedulerEninge {
//	private final Logger logger = LoggerFactory.getLogger(SchedulerEninge.class);
//
//	@Autowired
//	private StreamService streamService;
//
//	@Autowired
//	private KafkaListenerEndpointRegistry registry;
//
//	@Value("${consumer.client.id}")
//	private String consumerClientId;
//
//	@Value("${dir.failed.mansek}")
//	private String dirFailedMansek;
//
//	@Value("${retention.file.failed.mansek}")
//	private int retentionFileFailedMansek;
//
//	/**
//    * @author BWX
//    * @Date 2020-03-12
//    * Scheduler for healtCheck API Destination Mansek
//    */
//	@Scheduled(fixedDelayString = "${fixed.delay.string}" )
//	public void healtCheckDestination() {
//		try {
//			String healt = streamService.healtCheck();
//			logger.info("health : "+ healt);
//			if ((healt != null) && (!registry.getListenerContainer(consumerClientId).isRunning())) {
//				logger.info("start engine consumer mansek..");
//				registry.getListenerContainer(consumerClientId).start();
//			} else if ((healt == null)
//					&& (registry.getListenerContainer(consumerClientId).isRunning())) {
//				logger.info("stop engine consumer mansek..");
//				registry.getListenerContainer(consumerClientId).stop();
//			}
//		} catch (Exception e) {
//			logger.error("Error healthcheck mansek....!!! "+  e.getCause());
//		}
//	}
//
//	/**
//    * @Date 2020-12-10
//    * Scheduler for retention file @prefixDate-failed-mansek  </br>
//    * directory file in properties file (dir.failed.mansek)  </br>
//    * retention file failed-mansek in properties file (retention.file.failed.mansek) in day  </br>
//    */
//	@Scheduled(cron = "${cron.retention.file.failed.mansek}")
//	public void retentionFile() {
//		try {
//			if(retentionFileFailedMansek > 0) {
//				SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
//				File folder = new File(dirFailedMansek);
//				for (File file : folder.listFiles()) {
//					if (file.getName().contains("failed-mansek")) {
//						String dateString = file.getName().substring(0, 8);
//						Date date1 = sdf1.parse(dateString);
//						Calendar c = Calendar.getInstance();
//						c.setTime(new Date());
//						c.add(Calendar.DAY_OF_MONTH, (retentionFileFailedMansek * -1));
//						Date date2 = c.getTime();
//						if (date1.compareTo(date2) < 0) {
//							Boolean result = file.delete();
//							if(result) {
//								logger.info("success remove file " + file.getName());
//							}else {
//								logger.info("failed remove file " + file.getName());
//							}
//						}
//					}
//				}
//			}else {
//				logger.info("retention not execute because retention.file.failed.mansek = "+ retentionFileFailedMansek);
//			}
//		} catch (Exception e) {
//			logger.error("Error can't remove file " + e.getCause());
//		}
//	}
//}
