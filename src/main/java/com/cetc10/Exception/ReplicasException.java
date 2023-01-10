package com.cetc10.Exception;

public class ReplicasException extends RuntimeException {
    private String message;

    public ReplicasException(String message){
        super(message);
        this.message = message;
    }

    public String getMessage(){
        return message;
    }
}
