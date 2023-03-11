#!/usr/bin/env python

import pricing.pricing as pricing
from tkinter import *
from tkinter import messagebox


def start():
    """Calculates pricing"""

    global curr_settings
    curr_settings = None

    window = Tk()
    window.title("Ценообразование (v.0.1)")
    window.geometry('330x50')

    def settings():
        global curr_settings

        distances = ''
        prices = ''
        dist = ''
        price = ''
        deviation = ''

        if curr_settings:
            distances = ','.join(str(el) for el in curr_settings.distances)
            prices = ','.join(str(el) for el in curr_settings.prices)
            dist = str(curr_settings.def_unit)
            price = str(curr_settings.unit_price)
            deviation = str(curr_settings.deviation)

        settings_window = Tk()
        settings_window.title("Настройки")
        settings_window.geometry('350x200')

        prices_lbl = Label(settings_window, text="Диапазоны цен")
        prices_lbl.grid(column=0, row=0, sticky=W, padx=5, pady=10)

        prices_value = Entry(settings_window, width=30)
        prices_value.insert(0, prices)
        prices_value.grid(column=1, row=0, pady=10)

        distances_lbl = Label(settings_window, text="Диапазоны расстояний")
        distances_lbl.grid(column=0, row=1, sticky=W, padx=5)

        distances_value = Entry(settings_window, width=30)
        distances_value.insert(0, distances)
        distances_value.grid(column=1, row=1)

        dist_lbl = Label(settings_window, text="Шаг расстояния")
        dist_lbl.grid(column=0, row=2, sticky=W, padx=5, pady=10)

        dist_value = Entry(settings_window, width=5)
        dist_value.insert(0, dist)
        dist_value.grid(column=1, row=2, sticky=W, pady=10)

        price_lbl = Label(settings_window, text="Цена за шаг")
        price_lbl.grid(column=0, row=3, sticky=W, padx=5)

        price_value = Entry(settings_window, width=5)
        price_value.insert(0, price)
        price_value.grid(column=1, row=3, sticky=W)

        dev_lbl = Label(settings_window, text="Отклонение")
        dev_lbl.grid(column=0, row=4, sticky=W, padx=5, pady=10)

        dev_value = Entry(settings_window, width=5)
        dev_value.insert(0, deviation)
        dev_value.grid(column=1, row=4, sticky=W)

        def save_settings():
            global curr_settings

            new_distances = tuple([int(el) for el in distances_value.get().split(',')])
            new_prices = tuple([int(el) for el in prices_value.get().split(',')])
            new_dist = float(dist_value.get())
            new_price = float(price_value.get())
            new_deviation = float(dev_value.get())

            curr_settings = pricing.PricingSettings(
                new_prices,
                new_distances,
                new_dist,
                new_price,
                new_deviation
            )

            settings_window.destroy()

        save_btn = Button(settings_window, text='Сохранить', command=save_settings)
        save_btn.grid(column=0, row=5, columnspan=2, padx=15)

        settings_window.mainloop()

    menu = Menu(window)
    menu.add_command(label='Настройки', command=settings)

    window.config(menu=menu)

    ent_lbl = Label(window, text="Сеть")
    ent_lbl.grid(column=0, row=0, sticky=W, padx=5, pady=10)

    ent_value = Entry(window, width=10)
    ent_value.grid(column=1, row=0, pady=10)

    pharm_lbl = Label(window, text="Аптека")
    pharm_lbl.grid(column=2, row=0, sticky=W, padx=5, pady=10)

    pharm_value = Entry(window, width=15)
    pharm_value.grid(column=3, row=0, pady=10)

    def calculate():
        global curr_settings

        ent_str = ent_value.get()
        pharm_str = pharm_value.get()
        if not ent_str or not pharm_str:
            return

        enterprise = int(ent_str)
        serial_number = int(pharm_str)
        new_pricing = pricing.GoodsPricing(enterprise, serial_number, curr_settings)

        if not new_pricing.id_pharmacy:
            return

        if not new_pricing.settings:
            return

        curr_settings = new_pricing.settings

        new_pricing.recalculate()
        new_pricing.make_pricing()

        messagebox.showinfo('Готово', 'Расчет выполнен')

    btn = Button(window, text='Расчет', command=calculate)
    btn.grid(column=4, row=0, padx=15, pady=10)

    window.mainloop()


def main():
    start()


if __name__ == '__main__':
    main()
