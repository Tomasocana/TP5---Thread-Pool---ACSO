# Compiler settings
CXX = g++
CXXFLAGS = -std=c++11 -Wall -pthread -g

# Dependencias comunes (Tu librería)
# Estos archivos se usan en TODOS los tests
DEPS = thread-pool.cc Semaphore.cc

# Nombres de los ejecutables que queremos crear
TARGETS = maintest tpcustom tptest

# Regla "all": Si escribes 'make', compila todo
all: $(TARGETS)

# 1. Regla para el test principal (main.cc)
maintest: main.cc $(DEPS)
	$(CXX) $(CXXFLAGS) -o maintest main.cc $(DEPS)

# 2. Regla para el test custom (tpcustomtest.cc)
tpcustom: tpcustomtest.cc $(DEPS)
	$(CXX) $(CXXFLAGS) -o tpcustom tpcustomtest.cc $(DEPS)

# 3. Regla para el test oficial (tptest.cc)
tptest: tptest.cc $(DEPS)
	$(CXX) $(CXXFLAGS) -o tptest tptest.cc $(DEPS)

# Limpieza
clean:
	rm -f $(TARGETS)

.PHONY: all clean