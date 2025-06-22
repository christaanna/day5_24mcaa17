package org.example;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;


public class employee {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        MongoClient client = MongoClients.create("mongodb://admin:admin@172.21.14.92:27017,172.21.17.53:27017,172.21.17.54:27017/");
        MongoDatabase database = client.getDatabase("EmployeeDetails");
        MongoCollection<Document> collection = database.getCollection("employee");
        int choice;

        do {
            System.out.println("1.Add \n 2.Update \n 3.Delete \n 4.Search \n 5.List \n 6.department statistics");
            System.out.println("\n Enter your choice : ");
            choice = sc.nextInt();
            sc.nextLine();

            switch (choice) {
                case 1 :
                    System.out.println("Enter the number of employees you want to insert :");
                    int n = sc.nextInt();
                    sc.nextLine();

                    List<Document> stud = new ArrayList<>();
                    for (int i=0; i<n ;i++) {
                        System.out.println("Enter Employee name : ");
                        String name = sc.nextLine();
                        System.out.println("Enter EmailId : ");
                        String email = sc.nextLine();
                        if (collection.find(eq("email", email)).first() != null) {
                            System.out.println("Error: Email " + email + " already exists. Skipping this entry.");
                            continue;
                        }
//                        System.out.println("Enter phone number: ");
//                        int phone = sc.nextInt();
//                        sc.nextLine();
                        System.out.println("Enter Skill : ");
                        String skill = sc.nextLine();
                        System.out.println("Enter Department : ");
                        String department = sc.nextLine();
                        System.out.println("Enter Joining Date : ");
                        String jod = sc.nextLine();

                        Document doc = new Document("name", name).append("email", email).append("skills", skill).append("department", department).append("joiningDate", jod);
                        stud.add(doc);
                    }
                    collection.insertMany(stud);
                    break;



                case 2 :
                    System.out.println("Enter Employee name to update: ");
                    String name = sc.nextLine();
                    Document name1 = collection.find(new Document("name", name)).first();

                    if (name1 == null) {
                        System.out.println("Employee with name " + name+ " not found.");
                        break;
                    }
                    System.out.println("Enter updated Skill : ");
                    String skill = sc.nextLine();
                    System.out.println("Enter updated Department : ");
                    String department = sc.nextLine();


                    Bson filter = eq("name",name);
                    Bson update = combine(set("skills", skill),set("department",department));
                    FindOneAndUpdateOptions options = new FindOneAndUpdateOptions()
                            .returnDocument(ReturnDocument.AFTER);
                    Document updatedDoc = collection.findOneAndUpdate(filter, update,options);
                    if (updatedDoc != null) {
                        System.out.println("\nUpdated Employee:");
                        System.out.println(updatedDoc.toJson());
                    } else {
                        System.out.println("No document found to update.");
                    }

                    break;

                case 3 :
                    System.out.println("Enter EmailId : ");
                    String email = sc.nextLine();
                    Document docs = collection.find(new Document("email", email)).first();
                    if (docs != null) {
                        collection.deleteOne(eq("email",email));
                        System.out.println("\nDeleted Employee:");
                        System.out.println(docs.toJson());
                    } else {
                        System.out.println("No document found to update.");
                    }

                    break;

                case 4:
    System.out.println("Search by field (name/email/department/skills/joiningDate): ");
    String searchField = sc.nextLine();

    Bson filter = null;

    switch (searchField.toLowerCase()) {
        case "name":
            System.out.println("Enter partial or full name to search: ");
            String nameInput = sc.nextLine();
            filter = Filters.regex("name", Pattern.compile(nameInput, Pattern.CASE_INSENSITIVE));
            break;

        case "email":
            System.out.println("Enter email to search: ");
            String emailInput = sc.nextLine();
            filter = Filters.eq("email", emailInput);
            break;

        case "department":
            System.out.println("Enter department to search: ");
            String deptInput = sc.nextLine();
            filter = Filters.eq("department", deptInput);
            break;

        case "skills":
            System.out.println("Enter skill to search for: ");
            String skillInput = sc.nextLine();
            filter = Filters.elemMatch("skills", Filters.eq("$eq", skillInput));
            break;

        case "joiningdate":
            System.out.println("Enter start date (yyyy-MM-dd): ");
            String start = sc.nextLine();
            System.out.println("Enter end date (yyyy-MM-dd): ");
            String end = sc.nextLine();

            // Convert to Date objects
            try {
                Date startDate = new SimpleDateFormat("yyyy-MM-dd").parse(start);
                Date endDate = new SimpleDateFormat("yyyy-MM-dd").parse(end);
                filter = Filters.and(
                        Filters.gte("joiningDate", startDate),
                        Filters.lte("joiningDate", endDate)
                );
            } catch (ParseException e) {
                System.out.println("Invalid date format.");
                break;
            }
            break;

        default:
            System.out.println("Invalid field entered.");
            break;
    }

    if (filter != null) {
        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            System.out.println(doc.toJson());
        }
    }
    break;


                case 5 :
                    int page = 1;
                    int pageSize = 5;
                    System.out.println("Sort by: 1) Name  2) Joining Date");
                    int sortOption = sc.nextInt();

                    Bson sortField;
                    if (sortOption == 1) {
                        sortField = Sorts.ascending("name");
                    } else {
                        sortField = Sorts.ascending("joiningDate");
                    }

                    FindIterable<Document> allEmployees = collection.find().sort(sortField).skip((page - 1) * pageSize).limit(pageSize);
                    for (Document doc : allEmployees) {
                        System.out.println(doc.toJson());
                    }
                    break;

                case 6 :

                    List<Bson> pipeline = List.of(
                            new Document("$group", new Document("_id", "$department")
                                    .append("count", new Document("$sum", 1)))
                    );
                    collection.aggregate(pipeline).forEach(doc -> {
                        System.out.println("Department: " + doc.getString("_id") + ", Count: " + doc.getInteger("count"));
                    });
                    break;

                default:
                    System.out.println("Invalid choice.");
            }


        }while(choice != 6);
        client.close();
    }
}





