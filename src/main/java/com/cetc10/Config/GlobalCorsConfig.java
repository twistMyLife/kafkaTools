package com.cetc10.hjj.jumpBox.conf;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 全局MVC配置
 * @author GaoLe
 * @date 2022/11/18
 */
@Configuration
public class GlobalCorsConfig implements WebMvcConfigurer{


    /**
     * 解决跨域请求
     * @param registry CorsRegistry
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        //添加映射路径
        registry.addMapping("/**")
                //放行哪些域
                .allowedOrigins("*")
                //是否发送Cookie
                .allowCredentials(true)
                //放行哪些请求方式
                .allowedMethods("GET", "POST", "PUT", "DELETE")
                //放行哪些原始请求头信息
                .allowedHeaders("*")
                //暴露哪些头部信息(不设置亦可)
                .exposedHeaders("Header1", "Header2");
    }

    /**
     * 处理静态资源加载(所有 ResourceHandler里的请求，都会去ResourceLocations中查找资源)
     * @param registry ResourceHandlerRegistry
     */
//    @Override
//    public void addResourceHandlers(ResourceHandlerRegistry registry) {
//        registry.addResourceHandler("/static/**")
//                .addResourceLocations("classpath:/static/");
//        registry.addResourceHandler("/templates/**")
//                .addResourceLocations("classpath:/templates/");
//    }

    //@Bean
    //public CorsFilter corsFilter(){
    //    //1. 添加cors配置信息
    //    CorsConfiguration config = new CorsConfiguration();
    //    //放行哪些原始域
    //    config.addAllowedOrigin("*");
    //    //放行哪些请求方式
    //    config.addAllowedMethod("*");
    //    //放行哪些原始请求头信息
    //    config.addAllowedHeader("*");
    //    //暴露哪些头部信息
    //    config.addExposedHeader(HttpHeaders.ACCEPT);
    //    //是否发送Cookie
    //    config.setAllowCredentials(true);
    //    //2. 添加映射路径
    //    UrlBasedCorsConfigurationSource corsConfigurationSource = new UrlBasedCorsConfigurationSource();
    //    corsConfigurationSource.registerCorsConfiguration("/**", config);
    //    //3. 返回CorsFilter
    //    return new CorsFilter(corsConfigurationSource);
    //}

}
