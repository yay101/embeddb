package main

import (
	"fmt"
	"log"
	"os"

	"github.com/yay101/embeddb"
)

// Person is our example struct for the database
type Person struct {
	ID       uint32 `db:"id,primary"`   // Primary key
	Name     string `db:"index"`        // Indexed field
	Email    string `db:"unique,index"` // Unique and indexed
	Age      int    // Not indexed by default
	IsActive bool
	Address  Address // Nested struct
}

// Address demonstrates nested struct support
type Address struct {
	Street  string
	City    string `db:"index"` // Can index nested fields too
	Country string
	ZipCode string
}

func main() {
	// Clean up any existing database files from previous runs
	os.Remove("people.db")
	os.Remove("people.db.Name.idx")
	os.Remove("people.db.Email.idx")
	os.Remove("people.db.Address.City.idx")

	// Create a new database with migration enabled (true) and auto-indexing enabled (true)
	db, err := embeddb.New[Person]("people.db", true, true)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	// Remember to close the database when done
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		} else {
			log.Println("Database closed successfully")
		}
	}()

	// Manually create an index for the Age field
	if err := db.CreateIndex("Age"); err != nil {
		log.Fatalf("Failed to create index on Age: %v", err)
	}
	log.Println("Created index on Age field")

	// Insert some sample data
	people := []Person{
		{
			Name:     "John Doe",
			Email:    "john@example.com",
			Age:      30,
			IsActive: true,
			Address: Address{
				Street:  "123 Main St",
				City:    "New York",
				Country: "USA",
				ZipCode: "10001",
			},
		},
		{
			Name:     "Jane Smith",
			Email:    "jane@example.com",
			Age:      25,
			IsActive: true,
			Address: Address{
				Street:  "456 Park Ave",
				City:    "Boston",
				Country: "USA",
				ZipCode: "02108",
			},
		},
		{
			Name:     "Bob Johnson",
			Email:    "bob@example.com",
			Age:      45,
			IsActive: false,
			Address: Address{
				Street:  "789 Broadway",
				City:    "New York",
				Country: "USA",
				ZipCode: "10003",
			},
		},
		{
			Name:     "Alice Brown",
			Email:    "alice@example.com",
			Age:      35,
			IsActive: true,
			Address: Address{
				Street:  "101 State St",
				City:    "Chicago",
				Country: "USA",
				ZipCode: "60601",
			},
		},
		{
			Name:     "Charlie Wilson",
			Email:    "charlie@example.com",
			Age:      28,
			IsActive: true,
			Address: Address{
				Street:  "202 Market St",
				City:    "San Francisco",
				Country: "USA",
				ZipCode: "94105",
			},
		},
	}

	// Insert the records
	for _, person := range people {
		id, err := db.Insert(&person)
		if err != nil {
			log.Printf("Failed to insert %s: %v", person.Name, err)
		} else {
			log.Printf("Inserted %s with ID %d", person.Name, id)
		}
	}

	// Basic retrieval by ID
	firstPerson, err := db.Get(1)
	if err != nil {
		log.Printf("Failed to retrieve person with ID 1: %v", err)
	} else {
		fmt.Printf("Retrieved person by ID: %+v\n", firstPerson)
	}

	// Query examples using indexes

	// 1. Query by Name (string field)
	fmt.Println("\n--- Querying by Name ---")
	nameResults, err := db.Query("Name", "John Doe")
	if err != nil {
		log.Printf("Query by name failed: %v", err)
	} else {
		fmt.Printf("Found %d people named 'John Doe':\n", len(nameResults))
		for _, person := range nameResults {
			fmt.Printf("  %+v\n", person)
		}
	}

	// 2. Query by Age (int field)
	fmt.Println("\n--- Querying by Age ---")
	ageResults, err := db.Query("Age", 25)
	if err != nil {
		log.Printf("Query by age failed: %v", err)
	} else {
		fmt.Printf("Found %d people age 25:\n", len(ageResults))
		for _, person := range ageResults {
			fmt.Printf("  %s (%d)\n", person.Name, person.Age)
		}
	}

	// 3. Query by City (nested field)
	fmt.Println("\n--- Querying by City (nested field) ---")
	cityResults, err := db.Query("Address.City", "New York")
	if err != nil {
		log.Printf("Query by city failed: %v", err)
	} else {
		fmt.Printf("Found %d people in New York:\n", len(cityResults))
		for _, person := range cityResults {
			fmt.Printf("  %s (%s)\n", person.Name, person.Address.City)
		}
	}

	// Update a record
	fmt.Println("\n--- Updating a Record ---")
	if personToUpdate, err := db.Get(1); err == nil {
		personToUpdate.Age = 31
		personToUpdate.Address.ZipCode = "10002"

		if err := db.Update(1, personToUpdate); err != nil {
			log.Printf("Failed to update person: %v", err)
		} else {
			log.Println("Updated person successfully")

			// Verify the update
			if updated, err := db.Get(1); err == nil {
				fmt.Printf("Updated person: %+v\n", updated)
			}
		}
	}

	// Delete a record
	fmt.Println("\n--- Deleting a Record ---")
	if err := db.Delete(3); err != nil {
		log.Printf("Failed to delete person: %v", err)
	} else {
		log.Println("Deleted person with ID 3")

		// Verify the delete
		if deleted, err := db.Get(3); err != nil {
			log.Println("Person was successfully deleted (not found)")
		} else {
			fmt.Printf("Warning: Person was not deleted: %+v\n", deleted)
		}
	}

	// Demonstrate database vacuum
	fmt.Println("\n--- Vacuuming the Database ---")
	if err := db.Vacuum(); err != nil {
		log.Printf("Failed to vacuum database: %v", err)
	} else {
		log.Println("Database vacuumed successfully")
	}

	// Final database status
	fmt.Println("\n--- All Records After Operations ---")
	for i := uint32(1); i <= 5; i++ {
		if person, err := db.Get(i); err == nil {
			fmt.Printf("ID %d: %s (Age: %d, City: %s)\n",
				i, person.Name, person.Age, person.Address.City)
		} else {
			fmt.Printf("ID %d: Not found\n", i)
		}
	}
}
