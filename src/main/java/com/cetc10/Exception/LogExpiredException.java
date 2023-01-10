package com.cetc10.Exception;

public class LogExpiredException extends Exception {
    private String message;

    public LogExpiredException(String message){
        super(message);
        this.message = message;
    }

    public String getMessage(){
        return message;
    }

}
