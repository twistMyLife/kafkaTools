package com.cetc10.Exception;

public class CommitOffsetNotFoundException extends RuntimeException {

    private String message;

    public CommitOffsetNotFoundException(String message){
        super(message);
        this.message = message;
    }

    public String getMessage(){
        return message;
    }

}
