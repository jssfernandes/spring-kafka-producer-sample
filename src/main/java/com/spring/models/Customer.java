package com.spring.models;

import org.springframework.stereotype.Component;
import java.io.Serializable;


@Component
public class Customer implements Serializable {
    private String firstName;
    private String lastName;
    private String email;
    private String document;
    private String phone;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getDocument() {
        return document;
    }

    public void setDocument(String document) {
        this.document = document;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

//    @Override
//    public String toString() {
//        return "Customer [firstName=" + firstName + ", lastName=" + lastName + ", email=" + email
//                + ", document=" + document + ", phone=" + phone + "]";
//    }
}
