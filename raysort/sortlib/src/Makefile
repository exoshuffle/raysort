CXX = g++
CXXOPT = -pthread -ggdb
# -O3

SRC = *.h *.cpp
TARGET = test

run: $(TARGET)
	./$(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXOPT) $(SRC) -o $(TARGET)

.PHONY: clean
clean:
	rm $(TARGET)
