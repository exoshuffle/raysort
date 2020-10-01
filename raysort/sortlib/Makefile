CXX = g++

SRC = *.h *.cpp
TARGET = test

run: $(TARGET)
	./$(TARGET)

$(TARGET): $(SRC)
	$(CXX) -ggdb $(SRC) -o $(TARGET)

.PHONY: clean
clean:
	rm $(TARGET)
