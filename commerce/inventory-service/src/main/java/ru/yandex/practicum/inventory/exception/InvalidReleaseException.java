package ru.yandex.practicum.inventory.exception;

public class InvalidReleaseException extends RuntimeException {

    public InvalidReleaseException(String message) {
        super(message);
    }
}
