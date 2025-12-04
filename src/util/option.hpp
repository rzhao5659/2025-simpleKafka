#ifndef __OPTION_HPP__
#define __OPTION_HPP__

template<typename T>
class Option {
private:
    bool has_value_;
    T value_;

public:
    Option() : has_value_(false), value_() {}

    template<typename U>
    void setValue(U&& value) {
        this->value_ = std::forward<U>(value);
        this->has_value_ = true;
    }

    bool hasValue() {
        return has_value_;
    }

    T getValue() {
        return value_;
    }
};

#endif