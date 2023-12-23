//package com.microservices.demo.elastic.query.service.config;
//
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
//import org.springframework.security.config.annotation.web.builders.HttpSecurity;
//import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
//import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
//import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
//import org.springframework.security.crypto.password.PasswordEncoder;
//import com.microservices.demo.twitter.to.kafka.config.UserConfigData;
//
//@Configuration
//@EnableWebSecurity
//public class WebSecurityConfig extends WebSecurityConfigurerAdapter {
//
//    private final UserConfigData userConfigData;
//
//    public WebSecurityConfig(UserConfigData userData) {
//        this.userConfigData = userData;
//    }
//
//    @Override
//    public void configure(HttpSecurity http) throws Exception {
//        http
//                .httpBasic()
//                .and()
//                .authorizeRequests()
//                .antMatchers("/**").hasRole("USER")
//                .and()
//                .csrf().disable();
//    }
//
//    @Override
//    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
//        auth.inMemoryAuthentication()
//                .withUser(userConfigData.getUserName())
//                .password(passwordEncoder().encode(userConfigData.getPassword()))
//                .roles(userConfigData.getRoles());
//    }
//
//    @Bean
//    protected PasswordEncoder passwordEncoder() {
//        return new BCryptPasswordEncoder();
//    }
//
//
//}
