#if !defined(_BDCI19_STR_H_INCLUDED_)
#define _BDCI19_STR_H_INCLUDED_

//
// Compile-time string implementation
//

template<size_t _N>
struct string
{
    static_assert(_N > 0, "At least NULL terminator is required");

    constexpr explicit string(const char (&s)[_N]) noexcept
    {
        for (size_t i = 0; i < (_N - 1); ++i) {
            value[i] = s[i];
        }
        value[_N - 1] = '\0';
    }

    [[nodiscard]]
    __always_inline
    constexpr bool operator ==(const char* other) const noexcept
    {
        return std::string_view(value, _N - 1) == other;
    }

    template<typename TOther>
    [[nodiscard]]
    __always_inline
    constexpr bool operator !=(TOther&& other) const noexcept
    {
        return !(*this == std::forward<TOther>(other));
    }

public:
    char value[_N] { };
};


template<size_t _N>
constexpr string<_N> make_string(const char (&s)[_N]) noexcept
{
    return string<_N>(s);
}

constexpr string<1> make_string() noexcept
{
    return make_string("");
}


#endif  // !defined(_BDCI19_STR_H_INCLUDED_)
