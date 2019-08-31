package org.gft.big.data.practice.kafka.academy.model;

public class User {

    private long id;

    private String name;

    private String surname;

    private int age;

    public User() {
        ;
    }

    public User(long id, String name, String surname, int age) {
        this.id = id;
        this.name = name;
        this.surname = surname;
        this.age = age;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public int getAge() {
        return age;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString(){
        return "User[id=" + id + ", name=" + name + ", surname=" + surname + ", age=" + age + "]";
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof User) {
            User u = (User) o;
            return u.id == id && u.name.equals(name) && u.surname.equals(surname) && u.age == age;
        } else {
            return false;
        }
    }
}
